package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zYoma/yandex_kafka/internal/application"
	"github.com/zYoma/yandex_kafka/internal/application/config"
	"github.com/zYoma/yandex_kafka/internal/domain"
	"github.com/zYoma/yandex_kafka/internal/logger"
)

// Таймаут для запроса
const timeoutMs = 100

// Таймаут для запроса poll
const batchTimeoutMs = 5000

// Размер батча
const batchSize = 100

// KafkaConsumer структура клиента
type KafkaConsumer struct {
	Consumer     *kafka.Consumer
	Deserializer *jsonschema.Deserializer
	Config       *config.Config
}

type partitionGroup struct {
	tp   kafka.TopicPartition
	msgs []*kafka.Message
}

// NewKafkaConsumer создает новый экземпляр KafkaConsumer
func NewKafkaConsumer(deserializer *jsonschema.Deserializer, cfg *config.Config) (*KafkaConsumer, error) {
	// Конфигурация для Kafka Consumer
	consumer, err := kafka.NewConsumer(cfg.GetConsumerConfig())
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании консьюмера: %w", err)
	}

	return &KafkaConsumer{Deserializer: deserializer, Consumer: consumer, Config: cfg}, nil
}

// Start запускает консьюмер в SingleMessage режиме.
func (c *KafkaConsumer) StartSingleMessage(ctx context.Context) error {
	defer c.Consumer.Close()

	// Подписываемся на топики
	err := c.Subscribe()
	if err != nil {
		return err
	}

	// Запускаем бесконечный цикл
	for {
		select {
		case <-ctx.Done():
			return application.ErrConsumerStopped

		default:

			// Делаем запрос на считывание сообщения из брокера
			msg, err := c.Consumer.ReadMessage(timeoutMs)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); !ok || !kafkaErr.IsTimeout() {
					logger.Get().Sugar().Infof("Consumer error: %v\n", err)
				}
				continue
			}

			// Десериализация сообщения
			var product domain.Product
			err = c.Deserializer.DeserializeInto(c.Config.Topic, msg.Value, &product)
			if err != nil {
				logger.Get().Sugar().Errorf("Deserialize error: %v\n", err)
				continue
			}

			logger.Get().Sugar().Infof("Got message: %+v", product)

			// тут будет обработка сообщения

		}
	}

}

// Subscribe подписывает consumer на указанные топики
func (c *KafkaConsumer) Subscribe() error {
	topics := []string{c.Config.Topic}
	err := c.Consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return fmt.Errorf("Невозможно подписаться на топик: %s\n", err)
	}
	return nil
}

// Start запускает консьюмер в BatchMessage режиме.
func (c *KafkaConsumer) StartBatchMessage(ctx context.Context) error {
	defer c.Consumer.Close()

	// Подписываемся на топики
	err := c.Subscribe()
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return application.ErrConsumerStopped
		default:
			// Получаем следующую пачку сообщений
			batchTimeout := time.Duration(batchTimeoutMs) * time.Millisecond
			deadline := time.Now().Add(batchTimeout)
			batch := make([]*kafka.Message, 0, batchSize)

			// Собираем пачку сообщений

			for len(batch) < batchSize {
				remaining := time.Until(deadline)
				if remaining <= 0 {
					break
				}

				ev := c.Consumer.Poll(int(remaining.Milliseconds()))
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					batch = append(batch, e)

				case kafka.Error:
					logger.Get().Sugar().Infof("Consumer error: %v\n", e)
					continue

				default:
					// другие события игнорируем
				}
			}

			// Если пачка пустая, продолжаем цикл
			if len(batch) == 0 {
				continue
			}

			// Группируем сообщения по партициям
			partitionBatches := make(map[string]*partitionGroup)
			for _, msg := range batch {
				topic := *msg.TopicPartition.Topic

				key := fmt.Sprintf("%s:%d", topic, int(msg.TopicPartition.Partition))

				grp, ok := partitionBatches[key]
				if !ok {
					grp = &partitionGroup{
						tp: kafka.TopicPartition{
							Topic:     msg.TopicPartition.Topic,
							Partition: msg.TopicPartition.Partition,
						},
						msgs: make([]*kafka.Message, 0, 4),
					}
					partitionBatches[key] = grp
				}
				grp.msgs = append(grp.msgs, msg)
			}

			// Создаем канал для получения результатов обработки
			resultChan := make(chan error, len(partitionBatches))

			for _, grp := range partitionBatches {
				// клонируем значения для горутины
				tp := grp.tp
				msgs := grp.msgs
				go func(tp kafka.TopicPartition, msgs []*kafka.Message) {
					resultChan <- c.processPartition(ctx, tp, msgs)
				}(tp, msgs)
			}

			// Ждем завершения обработки всех партиций
			for i := 0; i < len(partitionBatches); i++ {
				select {
				case err := <-resultChan:
					if err != nil {
						return err
					}
				case <-ctx.Done():
					return application.ErrConsumerStopped
				}
			}
		}
	}
}

// processPartition обрабатывает пачку сообщений для одной партиции
func (c *KafkaConsumer) processPartition(ctx context.Context, tp kafka.TopicPartition, msgs []*kafka.Message) error {
	// Получаем offset'ы для пачки
	firstOffset := msgs[0].TopicPartition.Offset
	lastOffset := msgs[len(msgs)-1].TopicPartition.Offset

	// Десериализация всех сообщений из пачки
	var products []domain.Product
	for _, msg := range msgs {
		var product domain.Product
		err := c.Deserializer.DeserializeInto(c.Config.Topic, msg.Value, &product)
		if err != nil {
			logger.Get().Sugar().Errorf("Deserialize error: %v\n", err)
			continue
		}
		products = append(products, product)
	}

	logger.Get().Sugar().Infof("Got %d messages from partition %d, first offset %d", len(products), tp.Partition, firstOffset)

	// обработка сообщений
	if domain.ProcessProducts(ctx, products) {
		return c.commitPartition(tp, lastOffset)
	} else {
		return c.rollbackPartition(tp, firstOffset)
	}
}

// commitPartition коммитит обработанные сообщения
func (c *KafkaConsumer) commitPartition(tp kafka.TopicPartition, lastOffset kafka.Offset) error {
	_, err := c.Consumer.CommitOffsets([]kafka.TopicPartition{{
		Topic:     tp.Topic,
		Partition: tp.Partition,
		Offset:    lastOffset + 1,
	}})
	if err != nil {
		logger.Get().Sugar().Errorf("Commit error for partition %d: %v\n", tp.Partition, err)
		// тут не критично продолжать, сообщения уже обработаны 1 раз
	} else {
		logger.Get().Sugar().Infof("коммитим offset %v, партиция %v", lastOffset+1, tp.Partition)
	}
	return nil
}

// rollbackPartition возвращает оффсет при неудачной обработке
func (c *KafkaConsumer) rollbackPartition(tp kafka.TopicPartition, firstOffset kafka.Offset) error {
	err := c.Consumer.Seek(kafka.TopicPartition{
		Topic:     tp.Topic,
		Partition: tp.Partition,
		Offset:    firstOffset,
	}, 0)
	if err != nil {
		// Проверяем, является ли ошибка "Outdated"
		if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrOutdated {
			logger.Get().Sugar().Warnf("Outdated offset error for partition %v: %v. Continuing with next partition", tp.Partition, err)
			return nil // Просто продолжаем, так как offset уже не актуален
		}
		logger.Get().Sugar().Errorf("Seek error for partition %v: %v\n", tp, err)
		// нельзя идти дальше, иначе потеряем сообщения
		return fmt.Errorf("error seek partition, %v", err)
	}
	logger.Get().Sugar().Infof("откатываем offset %v, партиция %v", firstOffset, tp.Partition)
	return nil
}
