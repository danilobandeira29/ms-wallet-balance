package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

var Consumer *kafka.Consumer

func init() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:29092",
		"group.id":          "wallet",
		"auto.offset.reset": "earliest",
	}
	fmt.Println("connecting to kafka")
	var err error
	Consumer, err = kafka.NewConsumer(configMap)
	log.SetOutput(os.Stdout)
	if err != nil {
		log.Fatalf("error when trying to create new consumer for kafka %v\n", err)
	}
	err = Consumer.SubscribeTopics([]string{"balances"}, nil)
	if err != nil {
		log.Fatalf("error when trying to subscribe for topic %v\n", err)
	}
}
