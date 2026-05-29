package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"github.com/abolcerek/learn-pub-sub-starter/internal/pubsub"
	"github.com/abolcerek/learn-pub-sub-starter/internal/routing"
	"github.com/abolcerek/learn-pub-sub-starter/internal/gamelogic"
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
	gamelogic.PrintServerHelp()
	exchange := routing.ExchangePerilTopic
	key := routing.GameLogSlug + "*"
	queueName := "game_logs"
	// queueType is 1 if it is durable, it is 2 if it is transient
	queueType := 1
	newConn, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}
	_, _, err = pubsub.DeclareAndBind(connection, exchange, queueName, key, pubsub.SimpleQueueType(queueType))
	var data routing.PlayingState
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		if words[0] == "pause" {
			fmt.Println("Sending a pause message...")
			data.IsPaused = true
				err = pubsub.PublishJSON(newConn, exchange, key, data)
				if err != nil {
					log.Fatal(err)
				}
		} else if words[0] == "resume" {
			fmt.Println("Sending a resume message...")
			data.IsPaused = false
				err = pubsub.PublishJSON(newConn, exchange, key, data)
				if err != nil {
					log.Fatal(err)
				}
		} else if words[0] == "quit" {
			break
		} else {
			fmt.Println("Unknown command")
		}
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println()
	fmt.Println("Program is shutting down...")
}
