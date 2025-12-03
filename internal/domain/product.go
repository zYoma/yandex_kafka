package domain

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/zYoma/yandex_kafka/internal/application/interfaces"
)

// Определение структуры сообщения
type Product struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

// GenerateAndSendProducts генерирует сообщения и отправляет их через Kafka
func GenerateAndSendProducts(ctx context.Context, producer interfaces.Producer) error {
	products := GenerateProducts()

	// Преобразуем []*Product в []interface{} для отправки
	messages := make([]interface{}, len(products))
	for i, product := range products {
		messages[i] = product
	}

	return producer.SendMessages(ctx, messages)
}

// GenerateProducts генерирует массив из 10000 продуктов
func GenerateProducts() []*Product {
	products := make([]*Product, 0, 10000)

	for i := 1; i <= 10000; i++ {
		product := &Product{
			Id:   i,
			Name: fmt.Sprintf("Product %d", i),
		}
		products = append(products, product)
	}

	return products
}

// ProcessProducts имитирует обработку списка продуктов с паузой
func ProcessProducts(ctx context.Context, products []Product) bool {
	// Имитация обработки с паузой
	time.Sleep(100 * time.Millisecond)

	// Возвращаем случайное значение успеха
	rand.Seed(time.Now().UnixNano())
	return rand.Float32() > 0.5
}
