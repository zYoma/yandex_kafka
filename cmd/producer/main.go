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

	// получаем конфигурацию продюсера
	config, err := config.GetConfig()
	if err != nil {
		panic(err)
	}

	// слушаем сигналы
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// получаем сериализатор
	serializer, _, err := kafka.SchemaMigration(config)
	if err != nil {
		panic(err)
	}

	// внедряем конкретную реализацию
	producerClient, err := kafka.NewKafkaProducer(serializer, config)
	if err != nil {
		panic(err)
	}
	defer producerClient.Stop()

	// создаем приложение
	producer := application.NewProducer(config, producerClient)

	// запускаем продюсер
	if err := producer.Run(ctx); err != nil {
		if errors.Is(err, application.ErrAppStopped) {
			logger.Get().Info("producer stopped")
			return
		}
		panic(err)
	}

}
