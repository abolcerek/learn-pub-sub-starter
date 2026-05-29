package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/abolcerek/learn-pub-sub-starter/internal/gamelogic"
	"github.com/abolcerek/learn-pub-sub-starter/internal/pubsub"
	"github.com/abolcerek/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const conn = "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(conn)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}
	exchange := routing.ExchangePerilDirect
	queueName := routing.PauseKey + "." + username
	routingKey := routing.PauseKey
	// queueType is 1 if it is durable, it is 2 if it is transient
	queueType := 2
	_, _, err = pubsub.DeclareAndBind(connection, exchange, queueName, routingKey, pubsub.SimpleQueueType(queueType))
	if err != nil {
		log.Fatal(err)
	}
	NewGameState := gamelogic.NewGameState(username)
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		if words[0] == "spawn" {
			err = NewGameState.CommandSpawn(words)
			if err != nil {
				log.Fatal(err)
			}
		} else if words[0] == "move" {
			move, err := NewGameState.CommandMove(words)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(move)
		} else if words[0] == "status" {
			NewGameState.CommandStatus()
		} else if words[0] == "help" {
			gamelogic.PrintClientHelp()
		} else if words[0] == "spam" {
			fmt.Println("Spamming not allowed yet!")
		} else if words[0] == "quit" {
			gamelogic.PrintQuit()
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
