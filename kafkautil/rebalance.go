package kafkautil

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func RebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		fmt.Printf("Rebalanced new partition(s) assigned=%v\n", ev.Partitions)
		// The application may update the start .Offset of each
		// assigned partition and then call IncrementalAssign().
		// Even though this example does not alter the offsets we
		// provide the call to IncrementalAssign() as an example.
		err := c.IncrementalAssign(ev.Partitions)
		if err != nil {
			panic(err)
		}
	case kafka.RevokedPartitions:
		fmt.Printf("Rebalanced old partition(s) revoked=%v\n", ev.Partitions)
		if c.AssignmentLost() {
			// Our consumer has been kicked out of the group and the entire assignment is thus lost.
			fmt.Println("Current assignment lost!")
		}
		// The client automatically calls IncrementalUnassign() unless the callback has already called that method.
	}
	return nil
}
