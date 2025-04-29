package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// func main() {
// 	consumerID := os.Getenv("CONSUMER_ID")

// 	c, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers": "localhost:9092",
// 		"group.id":          "streaming-consumer-group",
// 		"auto.offset.reset": "earliest",
// 	})
// 	if err != nil {
// 		panic(err)
// 	}

// 	c.SubscribeTopics([]string{"streaming-topic"}, nil)

// 	sigchan := make(chan os.Signal, 1)
// 	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

// 	fmt.Printf("Consumer %s started\n", consumerID)
// 	run := true
// 	for run {
// 		select {
// 		case sig := <-sigchan:
// 			fmt.Printf("Caught signal %v: terminating\n", sig)
// 			run = false
// 		default:
// 			ev := c.Poll(100)
// 			switch e := ev.(type) {
// 			case *kafka.Message:
// 				fmt.Printf("[Consumer %s] Message on %s: %s = %s\n", consumerID, e.TopicPartition, string(e.Key), string(e.Value))
// 			case kafka.Error:
// 				fmt.Fprintf(os.Stderr, "Error: %v\n", e)
// 			}
// 		}
// 	}
// 	c.Close()
// }

// Concurrency
var (
	numWorkers = 5
	maxRetries = 3
	dlqTopic   = "streaming-topic-dlq"
	count      = 0
)

func main() {
	consumerID := os.Getenv("CONSUMER_ID")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "streaming-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	err = c.SubscribeTopics([]string{"streaming-topic"}, nil)
	if err != nil {
		panic(err)
	}

	msgChan := make(chan *kafka.Message, 100)
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(consumerID, i, msgChan, &wg, p)
	}

	// Graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Consumer %s started with %d workers\n", consumerID, numWorkers)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: shutting down...\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				msgChan <- e
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Kafka error: %v\n", e)
			}
		}
	}

	close(msgChan)
	wg.Wait()
	p.Flush(5000)
	fmt.Println("Shutdown complete.")
}

func worker(consumerID string, id int, msgChan <-chan *kafka.Message, wg *sync.WaitGroup, producer *kafka.Producer) {
	defer wg.Done()
	for msg := range msgChan {
		count++
		log.Println("count", count)
		success := false
		for attempt := 1; attempt <= maxRetries; attempt++ {
			err := processMessage(consumerID, id, msg)
			if err == nil {
				success = true
				break
			}
			fmt.Printf("[Worker %d] Retry %d failed: %v\n", id, attempt, err)
			time.Sleep(time.Second * time.Duration(attempt)) // exponential backoff
		}

		if !success {
			fmt.Printf("[Worker %d] Sending message to DLQ: key=%s\n", id, msg.Key)
			_ = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &dlqTopic, Partition: kafka.PartitionAny},
				Key:            msg.Key,
				Value:          msg.Value,
			}, nil)
		}
	}
}

func processMessage(consumerID string, workerID int, msg *kafka.Message) error {
	fmt.Printf("[Consumer %s | Worker %d] %s = %s (Partition %d, Offset %d)\n",
		consumerID,
		workerID,
		string(msg.Key),
		string(msg.Value),
		msg.TopicPartition.Partition,
		msg.TopicPartition.Offset)

	// Simulate failure randomly (remove in prod)
	if string(msg.Key) == "fail" {
		return fmt.Errorf("simulated processing failure")
	}

	// TODO: real business logic here
	time.Sleep(500 * time.Millisecond)
	return nil
}
