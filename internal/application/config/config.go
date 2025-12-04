package config

import (
	"errors"

	env "github.com/caarlos0/env/v11"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Config представляет конфигурацию
type Config struct {
	BootstrapServers         string `env:"BOOTSTRAP_SERVER" envDefault:"kafka-0:9092,kafka-1:9092,kafka-2:9092"`
	Topic                    string `env:"TOPIC" envDefault:"test_topic"`
	Acks                     string `env:"ACKS" envDefault:"all"`
	CompressionType          string `env:"COMPRASSION_TYPE" envDefault:"zstd"`
	GroupId                  string `env:"GROUP_ID" envDefault:"test-group"`
	AutoOffsetReset          string `env:"AUTO_OFFSET_RESET" envDefault:"earliest"`
	EnableAutoCommit         bool   `env:"ENABLE_AUTO_COMMIT" envDefault:"false"`
	LingerMS                 int    `env:"LINGER_MS" envDefault:"5"`
	BatchNumMessage          int    `env:"BATCH_NUM_MESSAGE" envDefault:"10000"`
	DeliveryTimeoutMS        int    `env:"DELIVERY_TIMEOUT_MS" envDefault:"120000"`
	FetchWaitMaxMS           int    `env:"FETCH_WAIT_MAX_MS" envDefault:"100"`
	FetchMinByres            int    `env:"FETCH_MIN_BYRES" envDefault:"1"`
	Retries                  int    `env:"RETRIES" envDefault:"100"`
	MaxPollIntervalMS        int    `env:"MAX_POLL_INTERVAL_MS" envDefault:"300000"`
	SchemaRegistryServiceURL string `env:"SCHEMA_REGISTRY_SERVICE_URL" envDefault:"http://schema-registry:8081"`
	SingleMessageConsumer    bool   `env:"ENABLE_SINGLE_MESSAGE_CONSUMER" envDefault:"true"`
}

// GetConfig возвращает конфигурацию приложения из переменных окружения.
func GetConfig() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, errors.New("Config: failed to parse config")
	}
	return cfg, nil
}

// GetProducerConfig возвращает конфигурацию для продюсера Kafka
func (c *Config) GetProducerConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":   c.BootstrapServers,  // Адреса брокеров Kafka
		"compression.type":    c.CompressionType,   // Тип сжатия данных (none, gzip, snappy, lz4, zstd)
		"acks":                c.Acks,              // Уровень гарантии доставки (0, 1, all)
		"linger.ms":           c.LingerMS,          // подержать отправку, чтобы накопить батч
		"batch.num.messages":  c.BatchNumMessage,   // ограничение размера батча по сообщениям
		"delivery.timeout.ms": c.DeliveryTimeoutMS, // общий таймаут доставки
		"retries":             c.Retries,           // число ретраев при неудачной отправке
	}
}

// GetConsumerConfig возвращает конфигурацию для консьюмера Kafka
func (c *Config) GetConsumerConfig() *kafka.ConfigMap {
	enableAutoCommit := false
	if c.SingleMessageConsumer == true {
		enableAutoCommit = true
	}

	return &kafka.ConfigMap{
		"bootstrap.servers":    c.BootstrapServers,  // Адреса брокеров Kafka
		"group.id":             c.GroupId,           // ID группы потребителей
		"auto.offset.reset":    c.AutoOffsetReset,   // Политика сброса оффсетов при отсутствии сохраненных значений
		"enable.auto.commit":   enableAutoCommit,    // Включить автоматический коммит оффсетов
		"fetch.wait.max.ms":    c.FetchWaitMaxMS,    // Максимальное время ожидания данных при fetch запросе
		"fetch.min.bytes":      c.FetchMinByres,     // Минимальное количество байт, которое должно быть доступно для возврата
		"max.poll.interval.ms": c.MaxPollIntervalMS, // Максимальное время между вызовами на получение сообщений
	}
}
