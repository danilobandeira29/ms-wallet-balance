package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

func main() {
	consumerKafka()
	// todo
	// create multiplexer
	// create database in memory
	// create endpoint to get balance by account id
	// create docker image and publish
}

func consumerKafka() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:29092",
		"group.id":          "wallet",
		"auto.offset.reset": "earliest",
	}
	fmt.Println("connecting to kafka")
	consumer, err := kafka.NewConsumer(configMap)
	log.SetOutput(os.Stdout)
	if err != nil {
		log.Fatalf("error when trying to create new consumer for kafka %v\n", err)
	}
	defer func(c *kafka.Consumer) {
		if e := c.Close(); e != nil {
			log.Fatalf("error when trying to close consumer %v\n", e)
		}
	}(consumer)
	err = consumer.SubscribeTopics([]string{"balances"}, nil)
	if err != nil {
		log.Fatalf("error when trying to subscribe for topic %v\n", err)
	}
	for {
		mgs, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("error when trying to read message %v\n", err)
			continue
		}
		fmt.Printf("message received %s\n", string(mgs.Value))
	}
}
