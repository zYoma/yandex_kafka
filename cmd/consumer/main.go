package main

import (
	"context"
	"errors"
	"os/signal"
	"syscall"

	"github.com/zYoma/yandex_kafka/internal/application"
	"github.com/zYoma/yandex_kafka/internal/application/config"
	"github.com/zYoma/yandex_kafka/internal/infra/clients/kafka"
	"github.com/zYoma/yandex_kafka/internal/logger"
)

func main() {

	// получаем конфигурацию
	config, err := config.GetConfig()
	if err != nil {
		panic(err)
	}

	// слушаем сигналы
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// получаем десериализатор
	_, deserializer, err := kafka.SchemaMigration(config)
	if err != nil {
		panic(err)
	}

	// внедряем конкретные реализации
	consumerClient, err := kafka.NewKafkaConsumer(deserializer, config)
	if err != nil {
		panic(err)
	}

	// создаем приложение
	consumer := application.NewConsumer(config, consumerClient)

	// запускаем
	if err := consumer.Run(ctx); err != nil {
		if errors.Is(err, application.ErrAppStopped) {
			logger.Get().Info("consumer stopped")
			return
		}

		panic(err)
	}

}
