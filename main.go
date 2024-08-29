package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("Hellow go kafka")
	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		go SubscribeToTopic("my-topic")
		return c.SendString("Consumer is Initilized")
	})
	app.Listen(":3000")
	// produceMessaige()
}

func consumeMessaige() {
	topic := "my-topic"
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		fmt.Println("Error to connect kafka", err)
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6)
	b := make([]byte, 10e3)
	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b[:n]))
	}
	if err := batch.Close(); err != nil {
		fmt.Println("failed to close batch", err)
	}
	if err := conn.Close(); err != nil {
		fmt.Println("Failed to close connection:", err)
	}
}

func produceMessaige() {
	topic := "my-topic"
	partion := 0
	con, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partion)

	if err != nil {
		fmt.Println("Failed to connect kafka", err)
	}
	//send messaige to kafka
	con.SetWriteDeadline(time.Now().Add(60 * time.Second))
	tt, err := con.WriteMessages(
		kafka.Message{Value: []byte("Hellow from go application to kafka producer")},
	)
	fmt.Println("success", tt)
	if err != nil {
		fmt.Println("Failed to produce messaige")
	}
	if err := con.Close(); err != nil {
		fmt.Println("Falied to close messaige writer")
	}
}

func SubscribeToTopic(topic string) {
	// Kafka broker address
	brokerAddress := "localhost:9092"

	// Create a new Kafka reader with the broker address and topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		// GroupID allows for consumer group functionality
		// GroupID:   "my-group",
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		Partition: 0,    // You can handle multiple partitions here
	})

	// Make sure to close the reader when you're done
	defer reader.Close()

	// Create a context with a timeout to avoid blocking indefinitely
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()

	// Read the message
	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			break
		}

		// Print the message key and value
		fmt.Printf("received: %s => %s\n", string(message.Key), string(message.Value))
	}
}
