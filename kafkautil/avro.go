package kafkautil

import (
	"bytes"
	"fmt"
	"github.com/actgardner/gogen-avro/v10/compiler"
	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/resolver"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/actgardner/gogen-avro/v10/vm"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"os"
	"strings"
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

func Serialize(s *avro.SpecificSerializer, topic string, msg interface{}, key string, schemaName string) kafka.Message {
	payload, err := s.Serialize(topic, msg)
	if err != nil {
		fmt.Printf("Failed to serialize payload: %s\n", err)
		os.Exit(1)
	}
	return kafka.Message{
		Key:            []byte(key),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Headers:        []kafka.Header{{Key: "schema-name", Value: []byte(schemaName)}}}
}

func DeserializeInto(s *avro.SpecificDeserializer, topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}
	var avroMsg avro.SpecificAvroMessage
	switch t := msg.(type) {
	case avro.SpecificAvroMessage:
		avroMsg = t
	default:
		return fmt.Errorf("serialization target must be an avro message. Got '%v'", t)
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return err
	}
	reader, err := toAvroType(s, schemaregistry.SchemaInfo{Schema: avroMsg.Schema()})
	if err != nil {
		return err
	}
	for _, reference := range info.References {
		if strings.HasSuffix(reference.Name, reader.Name()) {
			metadata, err := s.Client.GetLatestSchemaMetadata(reference.Subject)
			if err != nil {
				return err
			}
			writer, err := toAvroType(s, schemaregistry.SchemaInfo{Schema: metadata.Schema})
			if err != nil {
				return err
			}
			deser, err := compiler.Compile(writer, reader)
			if err != nil {
				return err
			}
			r := bytes.NewReader(payload[5:])
			return vm.Eval(r, deser, avroMsg)
		}
	}
	return nil
}

func SchemaNameMatches(message *kafka.Message, schemaName string) bool {
	for _, header := range message.Headers {
		if header.Key == "schema-name" && string(header.Value) == schemaName {
			return true
		}
	}
	return false
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

func toAvroType(s *avro.SpecificDeserializer, schema schemaregistry.SchemaInfo) (schema.AvroType, error) {
	ns := parser.NewNamespace(false)
	return resolveAvroReferences(s.Client, schema, ns)
}

func resolveAvroReferences(c schemaregistry.Client, schema schemaregistry.SchemaInfo, ns *parser.Namespace) (schema.AvroType, error) {
	for _, ref := range schema.References {
		metadata, err := c.GetSchemaMetadata(ref.Subject, ref.Version)
		if err != nil {
			return nil, err
		}
		info := schemaregistry.SchemaInfo{
			Schema:     metadata.Schema,
			SchemaType: metadata.SchemaType,
			References: metadata.References,
		}
		_, err = resolveAvroReferences(c, info, ns)
		if err != nil {
			return nil, err
		}

	}
	sType, err := ns.TypeForSchema([]byte(schema.Schema))
	if err != nil {
		return nil, err
	}
	for _, def := range ns.Roots {
		if err := resolver.ResolveDefinition(def, ns.Definitions); err != nil {
			return nil, err
		}
	}
	return sType, nil
}
