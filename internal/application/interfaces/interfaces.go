package interfaces

import (
	"context"
)

// Producer предоставляет интерфейс продюсера.
type Producer interface {
	// SendMessages отправляет сообщения в топик
	SendMessages(ctx context.Context, messages []interface{}) error
}

// Consumer предоставляет интерфейс консьюмера.
type Consumer interface {
	// консюмер запускается в режиме поочередной обработки сообщений
	StartBatchMessage(ctx context.Context) error
	// консюмер запускается в режиме пакетной обработки сообщений
	StartSingleMessage(ctx context.Context) error
}
