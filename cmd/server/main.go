package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const conn = "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(conn)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	fmt.Println("Connection was successfull!")
	newConn, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}
	exchange := routing.ExchangePerilDirect
	key := routing.PauseKey
	data := routing.PlayingState{
		IsPaused: true,
	}
	err = pubsub.PublishJSON(newConn, exchange, key, data)
	if err != nil {
		log.Fatal(err)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println()
	fmt.Println("Program is shutting down...")
}
