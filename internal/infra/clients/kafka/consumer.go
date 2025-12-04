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

// Таймауты и параметры
const (
	timeoutMs      = 100
	batchSize      = 50
	batchTimeoutMs = 1000
)

// KafkaConsumer структура клиента
type KafkaConsumer struct {
	Consumer     *kafka.Consumer
	Deserializer *jsonschema.Deserializer
	Config       *config.Config
}

// partitionGroup структура для группирования сообщений по партициям
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
			return application.ErrAppStopped

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
			err = c.deserializeMessage(msg, &product)
			if err != nil {
				continue
			}

			logger.Get().Sugar().Infof("Got message: %+v", product)

			// тут будет обработка сообщения

		}
	}

}

// Десериализация сообщения
func (c *KafkaConsumer) deserializeMessage(msg *kafka.Message, data interface{}) error {
	err := c.Deserializer.DeserializeInto(c.Config.Topic, msg.Value, data)
	if err != nil {
		logger.Get().Sugar().Errorf("Deserialize error: %v\n", err)
		return err
	}
	return nil
}

// Subscribe подписывает consumer на указанные топики
func (c *KafkaConsumer) Subscribe() error {
	topics := []string{c.Config.Topic}
	err := c.Consumer.SubscribeTopics(topics, c.rebalanceCallback)
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
		// если контекст отменен - закрываем консьюмер и возвращаем ошибку
		case <-ctx.Done():
			return application.ErrAppStopped
		default:
			// Получаем следующую пачку сообщений

			// для того, чтобы ждать пока не наберется пачка нужного размера или не истечет время ожидания ее сбора
			batchTimeout := time.Duration(batchTimeoutMs) * time.Millisecond
			deadline := time.Now().Add(batchTimeout)
			batch := make([]*kafka.Message, 0, batchSize)

			// Собираем пачку сообщений
			for len(batch) < batchSize {
				remaining := time.Until(deadline)
				// если дедлайн вышел, прекращаем ждать новые сообщения
				if remaining <= 0 {
					break
				}

				ev := c.Consumer.Poll(timeoutMs)
				if ev == nil {
					time.Sleep(50 * time.Millisecond) // Задержка для снижения нагрузки
					continue
				}

				switch e := ev.(type) {
				// сообщения
				case *kafka.Message:
					batch = append(batch, e)
				//ошибки
				case kafka.Error:
					logger.Get().Sugar().Warnf("%% Error: %v: %v\n", e.Code(), e)
					if e.Code() == kafka.ErrAllBrokersDown {
						return fmt.Errorf("брокер недоступен, %v", e.Code())
					}
					continue
				// другие события игнорируем
				default:
					logger.Get().Sugar().Infof("Ignored %v\n", e)
				}
			}

			// Если пачка пустая, продолжаем цикл
			if len(batch) == 0 {
				continue
			}

			// Группируем сообщения по партициям чтобы паралельно их обработать
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

			// Создаем дочерний контекст для текущего батча
			ctxProcess, cancel := context.WithCancel(ctx)
			defer cancel()

			// Создаем канал для получения результатов обработки
			resultChan := make(chan error, len(partitionBatches))

			for _, grp := range partitionBatches {
				// распараллеливаем обработку партиций в горутинах
				tp := grp.tp
				msgs := grp.msgs
				go func(tp kafka.TopicPartition, msgs []*kafka.Message) {
					err := c.processPartition(ctxProcess, tp, msgs)
					select {
					case resultChan <- err:
					case <-ctxProcess.Done(): // Если контекст отменен, игнорируем результат
					}
				}(tp, msgs)
			}

			// Ждем завершения обработки всех партиций
			for i := 0; i < len(partitionBatches); i++ {
				select {
				case err := <-resultChan:
					if err != nil {
						cancel()
						return err
					}
				case <-ctx.Done():
					cancel()
					return application.ErrAppStopped
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
		err := c.deserializeMessage(msg, &product)
		if err != nil {
			continue
		}
		products = append(products, product)
	}

	logger.Get().Sugar().Infof("Got %d messages from partition %d, first offset %d", len(products), tp.Partition, firstOffset)

	// Создаем контекст с таймаутом, меньше чем max_poll_interval_ms
	maxPollInterval := time.Duration(c.Config.MaxPollIntervalMS) * time.Millisecond
	timeout := maxPollInterval - (maxPollInterval / 10) // Убираем 10% как запасной период

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Обработка сообщений.
	done := make(chan bool, 1)
	go func() {
		done <- domain.ProcessProducts(ctx, products)
	}()

	select {
	case success := <-done:
		if success {
			// Если обработка успешна, коммитит сообщения.
			return c.commitPartition(tp, lastOffset)
		} else {
			// Иначе - возвращаем весь батч на повторную обработку
			return c.rollbackPartition(tp, firstOffset)
		}
	case <-ctx.Done():
		// Таймаут превышен, возвращаем батч на повторную обработку
		logger.Get().Sugar().Warnf("Processing timeout for partition %d, rolling back", tp.Partition)
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
		// Может произойти перебалансировка перед seek, в таком случае пропускаем, так как партиции уже у другого консьюмера
		if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrOutdated {
			logger.Get().Sugar().Warnf("Outdated offset error for partition %v: %v. Continuing with next partition", tp.Partition, err)
			return nil
		}
		logger.Get().Sugar().Errorf("Seek error for partition %v: %v\n", tp, err)
		// нельзя идти дальше, иначе потеряем сообщения
		return fmt.Errorf("error seek partition, %v", err)
	}
	logger.Get().Sugar().Infof("откатываем offset %v, партиция %v", firstOffset, tp.Partition)
	return nil
}

func (c *KafkaConsumer) rebalanceCallback(k *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {

	// событие, когда консьюмер получил новые партиции
	case kafka.AssignedPartitions:
		logger.Get().Sugar().Infof("%% %s rebalance: %d new partition(s) assigned: %v\n",
			k.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		// событие, перед тем как консьюмер потеряет партитии
	case kafka.RevokedPartitions:
		logger.Get().Sugar().Infof("%% %s rebalance: %d partition(s) revoked: %v\n",
			k.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		if k.AssignmentLost() {
			// партиции могут уже быть отозваны для консьюмера, смещения по ним не удастся закоммитить
			logger.Get().Warn("Assignment lost involuntarily, commit may fail")
		}

	default:
		logger.Get().Sugar().Warnf("Unxpected event type: %v\n", event)
	}

	return nil
}
