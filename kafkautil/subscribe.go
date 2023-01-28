package kafkautil

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
)

func Subscribe(topic string, consumerGroup string, handleCallback func(*kafka.Message)) {
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	ccloudAPIKey := os.Getenv("CONFLUENT_CLOUD_API_KEY")
	ccloudAPISecret := os.Getenv("CONFLUENT_CLOUD_API_SECRET")
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"sasl.username":                 ccloudAPIKey,
		"sasl.password":                 ccloudAPISecret,
		"group.id":                      consumerGroup,
		"partition.assignment.strategy": "cooperative-sticky",
		"sasl.mechanisms":               "PLAIN",
		"security.protocol":             "SASL_SSL",
		"broker.address.family":         "v4",
		"auto.offset.reset":             "earliest"})
	if err != nil {
		fmt.Printf("Subscribe failed to create consumer with error=%s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Subscribe created consumer=%v\n", consumer)
	err = consumer.Subscribe(topic, RebalanceCallback)
	if err != nil {
		fmt.Printf("Subscribe failed to subscribe with error=%s\n", err)
	} else {
		Consume(consumer, handleCallback)
	}
	fmt.Println("Subscribe closing consumer")
	_ = consumer.Close()
}
