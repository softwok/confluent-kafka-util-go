package kafkautil

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
)

func NewProducer() *kafka.Producer {
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	ccloudAPIKey := os.Getenv("CONFLUENT_CLOUD_API_KEY")
	ccloudAPISecret := os.Getenv("CONFLUENT_CLOUD_API_SECRET")
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"sasl.username":     ccloudAPIKey,
		"sasl.password":     ccloudAPISecret,
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL"})
	if err != nil {
		fmt.Printf("NewProducer failed to create producer with error=%s\n", err)
		os.Exit(1)
	}
	fmt.Printf("NewProducer created producer=%v\n", producer)
	return producer
}

func Produce(p *kafka.Producer, msg *kafka.Message) {
	err := p.Produce(msg, nil)
	if err != nil {
		fmt.Printf("Produce failed to publish with error=%s\n", err)
		os.Exit(1)
	}
	// Wait for delivery report
	e := <-p.Events()
	message := e.(*kafka.Message)
	if message.TopicPartition.Error != nil {
		fmt.Printf("Produce failed to deliver message=%v\n", msg)
		os.Exit(1)
	} else {
		fmt.Printf("Produce delivered message=%v to topic=%s partition=%d at offset=%v\n", msg,
			*message.TopicPartition.Topic,
			message.TopicPartition.Partition,
			message.TopicPartition.Offset)
	}
}
