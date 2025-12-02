package logger

import (
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logger *zap.Logger
	once   sync.Once
)

// Get возвращает синглтон логгер
func Get() *zap.Logger {
	once.Do(func() {
		// Создаем конфигурацию для консольного логгера без JSON
		config := zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		config.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
		config.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

		var err error
		logger, err = config.Build()
		if err != nil {
			panic(err)
		}
	})
	return logger
}
