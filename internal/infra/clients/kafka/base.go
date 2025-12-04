package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	jsSchema "github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
	"github.com/invopop/jsonschema"
	"github.com/zYoma/yandex_kafka/internal/application/config"
	"github.com/zYoma/yandex_kafka/internal/domain"
	"github.com/zYoma/yandex_kafka/internal/logger"
)

// SchemaMigration мигрицая схемы в Schema Registry
func SchemaMigration(cfg *config.Config) (*jsSchema.Serializer, *jsSchema.Deserializer, error) {
	// Конфигурация для Schema Registry
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(cfg.SchemaRegistryServiceURL))
	if err != nil {
		return nil, nil, fmt.Errorf("ошибка при создании клиента Schema Registry: %w", err)
	}

	// Регистрация схемы с проверкой на изменения
	schemaId, err := RegisterSchemaFromStruct(srClient, cfg.Topic, domain.Product{})
	if err != nil {
		return nil, nil, fmt.Errorf("ошибка регистрации схемы: %w", err)
	}
	logger.Get().Sugar().Infof("Schema ID: %d", schemaId)

	// Создание сериализатора JSON
	serializer, err := jsSchema.NewSerializer(srClient, serde.ValueSerde, jsSchema.NewSerializerConfig())
	if err != nil {
		return nil, nil, fmt.Errorf("ошибка при создании сериализатора: %w", err)
	}

	// Создание десериализатора JSON
	deserializer, err := jsSchema.NewDeserializer(srClient, serde.ValueSerde, jsSchema.NewDeserializerConfig())
	if err != nil {
		return nil, nil, fmt.Errorf("ошибка при создании десериализатора: %w", err)
	}

	return serializer, deserializer, nil
}

// RegisterSchemaIfChanged регистрирует схему subject только если она изменилась.
// Возвращает id схемы (int) и ошибку.
func RegisterSchemaIfChanged(sr schemaregistry.Client, subject string, newSchemaStr string) (int, error) {
	// Собираем SchemaInfo.
	schemaInfo := schemaregistry.SchemaInfo{
		Schema:     newSchemaStr,
		SchemaType: "JSON",
	}

	// 1) Попробуем получить id уже идентичной схемы через GetID (normalize = true).
	if id, err := sr.GetID(subject, schemaInfo, true); err == nil {
		logger.Get().Sugar().Infof("Схема для субъекта %s уже зарегистрирована (id=%d)", subject, id)
		return id, nil
	}

	// 2) Получим все версии субъекта, чтобы узнать последнюю.
	versions, err := sr.GetAllVersions(subject)
	if err != nil {
		// Если субъекта нет — регистрируем новую схему.
		logger.Get().Sugar().Infof("Не удалось получить версии для субъекта %s: %v. Будем регистрировать новую схему.", subject, err)
		return registerSchema(sr, subject, schemaInfo)
	}

	if len(versions) == 0 {
		// Нет версий — регистрируем новую
		logger.Get().Sugar().Infof("Для субъекта %s нет версий, регистрируем новую схему", subject)
		return registerSchema(sr, subject, schemaInfo)
	}

	// 3) Получаем метаданные последней версии и сравниваем схемы
	lastVersion := versions[len(versions)-1]
	meta, err := sr.GetSchemaMetadata(subject, lastVersion)
	if err != nil {
		// Если не удалось получить метаданные — регистрируем новую
		logger.Get().Sugar().Infof("Не удалось получить метаданные последней версии (%d) для %s: %v. Регистрируем новую.", lastVersion, subject, err)
		return registerSchema(sr, subject, schemaInfo)
	}

	// Сравниваем строково.
	if meta.Schema == newSchemaStr {
		logger.Get().Sugar().Infof("Схема для субъекта %s не изменилась (id=%d, version=%d)", subject, meta.ID, meta.Version)
		return meta.ID, nil
	}

	// 4) Схема изменилась — регистрируем новую версию
	logger.Get().Sugar().Infof("Схема для субъекта %s изменилась (version %d -> новая), регистрируем новую версию", subject, lastVersion)
	return registerSchema(sr, subject, schemaInfo)
}

// registerSchema регистрирует схему
func registerSchema(sr schemaregistry.Client, subject string, schemaInfo schemaregistry.SchemaInfo) (int, error) {
	id, err := sr.Register(subject, schemaInfo, true)
	if err != nil {
		return 0, fmt.Errorf("ошибка регистрации схемы: %w", err)
	}
	return id, nil
}

// GenerateSchemaFromStruct генерирует JSON схему из Go структуры
func GenerateSchemaFromStruct(v interface{}) (string, error) {
	reflector := jsonschema.Reflector{}
	schema := reflector.Reflect(v)

	schemaBytes, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return "", err
	}

	return string(schemaBytes), nil
}

// RegisterSchemaFromStruct регистрирует схему, сгенерированную из структуры
func RegisterSchemaFromStruct(srClient schemaregistry.Client, subject string, v interface{}) (int, error) {
	schemaStr, err := GenerateSchemaFromStruct(v)
	if err != nil {
		return 0, err
	}

	return RegisterSchemaIfChanged(srClient, subject, schemaStr)
}
