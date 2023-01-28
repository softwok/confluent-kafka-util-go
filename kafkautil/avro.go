package kafkautil

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"os"
)

func NewValueAvroSpecificDeserializer() *avro.SpecificDeserializer {
	schemaRegistryAPIUrl := os.Getenv("SCHEMA_REGISTRY_URL")
	schemaRegistryAPIKey := os.Getenv("SCHEMA_REGISTRY_API_KEY")
	schemaRegistryAPISecret := os.Getenv("SCHEMA_REGISTRY_API_SECRET")
	client, err := schemaregistry.NewClient(schemaregistry.NewConfigWithAuthentication(
		schemaRegistryAPIUrl, schemaRegistryAPIKey, schemaRegistryAPISecret))
	if err != nil {
		fmt.Printf("NewValueAvroSpecificDeserializer failed to create schema registry client with error=%s\n", err)
		os.Exit(1)
	}
	deser, err := avro.NewSpecificDeserializer(client, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		fmt.Printf("NewValueAvroSpecificDeserializer failed to create deserializer with error=%s\n", err)
		os.Exit(1)
	}
	return deser
}
