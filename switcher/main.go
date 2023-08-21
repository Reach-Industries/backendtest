package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var switchWriter *kafka.Writer

func main() {
	fmt.Println("Starting Kafka Producer")
	switchWriter = getKafkaWriter("switch")
	writeLoop()
}

func getKafkaWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(getBrokerURLs()...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func getBrokerURLs() []string {
	BrokerURLs := strings.Split(os.Getenv("MSK_BROKERS"), ",")
	for i := range BrokerURLs {
		BrokerURLs[i] = strings.TrimSpace(BrokerURLs[i])
	}
	fmt.Println("Brokers:", BrokerURLs)
	return BrokerURLs
}

func writeLoop() {
	fmt.Println("Starting Write Loop")
	messages := []string{"Counter", "Text", "Time", "Dump"}

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	for {
		msg := messages[r.Intn(len(messages))]
		writeMessage(msg)
		fmt.Println("Wrote message:", msg)

		sleepTime := time.Duration(r.Intn(60-10+1)+10) * time.Second
		time.Sleep(sleepTime)
	}
}

func writeMessage(msg string) {
	if err := switchWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(time.Now().String()),
			Value: []byte(msg),
		},
	); err != nil {
		fmt.Println("Error while writing to kafka", err)
	}
}
