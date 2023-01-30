package kafkautil

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"os/signal"
	"syscall"
)

func NewConsumer(consumerGroup string) *kafka.Consumer {
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
		fmt.Printf("NewConsumer failed to create consumer with error=%s\n", err)
		os.Exit(1)
	}
	fmt.Printf("NewConsumer created consumer=%v\n", consumer)
	return consumer
}

func Consume(c *kafka.Consumer, handleCallback func(*kafka.Message)) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Consume terminating on signal=%v\n", sig)
			run = false
		default:
			event := c.Poll(100)
			if event == nil {
				continue
			}
			switch e := event.(type) {
			case *kafka.Message:
				handleCallback(event.(*kafka.Message))
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Printf("Consume terminating on event=%v code=%v\n", e, e.Code())
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Consume ignored event=%v\n", e)
			}
		}
	}
}

func Subscribe(topic string, consumerGroup string, handleCallback func(*kafka.Message)) {
	consumer := NewConsumer(consumerGroup)
	err := consumer.Subscribe(topic, RebalanceCallback)
	if err != nil {
		fmt.Printf("Subscribe failed to subscribe with error=%s\n", err)
		os.Exit(1)
	}
	Consume(consumer, handleCallback)
	fmt.Println("Subscribe closing consumer")
	_ = consumer.Close()
}
