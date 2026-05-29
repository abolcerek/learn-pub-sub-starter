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
	exchange := routing.ExchangePerilDirect
	key := routing.PauseKey
	newConn, err := connection.Channel()
	var data routing.PlayingState
	if err != nil {
		log.Fatal(err)
	}
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
