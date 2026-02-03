package main

import (
	"log"
	"math/rand"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	prod, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:58406",
		"acks":              "all"})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for e := range prod.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Fatalf("Failed to deliver message %v\n", ev.TopicPartition)
				} else {
					log.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	users := []string{"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"}
	items := []string{"book", "alarm clock", "t-shirts", "gift card", "batteries"}
	topic := "purchases"

	for n := 0; n < 10; n++ {
		key := users[rand.Intn(len(users))]
		data := items[rand.Intn(len(items))]
		err := prod.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(data)}, nil)
		if err != nil {
			log.Fatalf("Failed to produce message: %v\n", err)
		}
	}
	// Wait for all messages to be delivered
	prod.Flush(15 * 1000)
	prod.Close()
}
