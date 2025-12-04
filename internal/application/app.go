package application

import (
	"context"
	"errors"

	"github.com/zYoma/yandex_kafka/internal/application/config"
	"github.com/zYoma/yandex_kafka/internal/application/interfaces"
	"github.com/zYoma/yandex_kafka/internal/domain"
	"github.com/zYoma/yandex_kafka/internal/logger"
)

// ErrAppStopped возникает при остановке приложения.
var ErrAppStopped = errors.New("app stoped")

// ProducerApp представляет приложение продюсер.
type ProducerApp struct {
	Producer interfaces.Producer
	Config   *config.Config
}

// ConsumerApp представляет приложение консьюмер.
type ConsumerApp struct {
	Consumer interfaces.Consumer
	Config   *config.Config
}

// NewProducer создаёт новое приложение продюсер с настройками из конфигурации.
func NewProducer(cfg *config.Config, producer interfaces.Producer) *ProducerApp {
	return &ProducerApp{
		Producer: producer,
		Config:   cfg,
	}
}

// NewConsumer создаёт новое приложение консьюмер с настройками из конфигурации.
func NewConsumer(cfg *config.Config, consumer interfaces.Consumer) *ConsumerApp {
	return &ConsumerApp{
		Consumer: consumer,
		Config:   cfg,
	}
}

// // Run запускает приложение продюсер.
func (p *ProducerApp) Run(ctx context.Context) error {
	logger.Get().Info("run producer")
	err := domain.GenerateAndSendProducts(ctx, p.Producer)
	if err != nil {
		return err
	}
	return nil
}

// Run запускает приложение консьюмер.
func (c *ConsumerApp) Run(ctx context.Context) error {
	logger.Get().Sugar().Infof("run consumer, group_id: %v, topic: %v", c.Config.GroupId, c.Config.Topic)
	if c.Config.SingleMessageConsumer == true {
		return c.Consumer.StartSingleMessage(ctx)
	}
	return c.Consumer.StartBatchMessage(ctx)
}
