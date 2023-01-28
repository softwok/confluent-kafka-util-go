package kafkautil

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"os"
)

func NewValueAvroSpecificDeserializer() *avro.SpecificDeserializer {
	client := newSchemaRegistryClient()
	deser, err := avro.NewSpecificDeserializer(client, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		fmt.Printf("NewValueAvroSpecificDeserializer failed to create deserializer with error=%s\n", err)
		os.Exit(1)
	}
	return deser
}

func NewValueAvroSpecificSerializer() *avro.SpecificSerializer {
	client := newSchemaRegistryClient()
	ser, err := avro.NewSpecificSerializer(client, serde.ValueSerde, newSerializerConfig())
	if err != nil {
		fmt.Printf("NewValueAvroSpecificSerializer failed to create serializer with error=%s\n", err)
		os.Exit(1)
	}
	return ser
}

func newSchemaRegistryClient() schemaregistry.Client {
	schemaRegistryAPIUrl := os.Getenv("SCHEMA_REGISTRY_URL")
	schemaRegistryAPIKey := os.Getenv("SCHEMA_REGISTRY_API_KEY")
	schemaRegistryAPISecret := os.Getenv("SCHEMA_REGISTRY_API_SECRET")
	client, err := schemaregistry.NewClient(schemaregistry.NewConfigWithAuthentication(
		schemaRegistryAPIUrl, schemaRegistryAPIKey, schemaRegistryAPISecret))
	if err != nil {
		fmt.Printf("NewValueAvroSpecificSerializer failed to create schema registry client with error=%s\n", err)
		os.Exit(1)
	}
	return client
}

func newSerializerConfig() *avro.SerializerConfig {
	c := &avro.SerializerConfig{}
	c.AutoRegisterSchemas = false
	c.UseSchemaID = -1
	c.UseLatestVersion = true
	c.NormalizeSchemas = false
	return c
}
