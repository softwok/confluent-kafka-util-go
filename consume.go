package kafkautil

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"os/signal"
	"syscall"
)

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
