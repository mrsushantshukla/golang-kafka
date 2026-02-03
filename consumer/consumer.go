package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:58406",
		"group.id":          "kafka-go",
		"auto.offset.reset": "earliest"})
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Close()
	topic := "purchases"
	err = cons.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatal(err)
	}
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := cons.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			} else {
				fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
					*msg.TopicPartition.Topic, string(msg.Key), string(msg.Value))
			}
		}
	}
}
