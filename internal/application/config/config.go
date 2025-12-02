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
		"bootstrap.servers":   c.BootstrapServers,
		"compression.type":    c.CompressionType,
		"acks":                c.Acks,
		"linger.ms":           c.LingerMS,          // подержать отправку, чтобы накопить батч
		"batch.num.messages":  c.BatchNumMessage,   // ограничение размера батча по сообщениям
		"delivery.timeout.ms": c.DeliveryTimeoutMS, // общий таймаут доставки
	}
}

// GetConsumerConfig возвращает конфигурацию для консьюмера Kafka
func (c *Config) GetConsumerConfig() *kafka.ConfigMap {
	enableAutoCommit := false
	if c.SingleMessageConsumer == true {
		enableAutoCommit = true
	}

	return &kafka.ConfigMap{
		"bootstrap.servers":  c.BootstrapServers,
		"group.id":           c.GroupId,
		"auto.offset.reset":  c.AutoOffsetReset,
		"enable.auto.commit": enableAutoCommit,
	}
}
