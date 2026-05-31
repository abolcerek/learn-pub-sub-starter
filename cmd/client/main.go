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
	NewGameState := gamelogic.NewGameState(username)
	err = CreateAndBind("pause", username, connection, NewGameState)
	if err != nil {
		log.Fatal(err)
	}
	err = CreateAndBind("army", username, connection, NewGameState)
	if err != nil {
		log.Fatal(err)
	}
	newConn, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		if words[0] == "spawn" {
			err = NewGameState.CommandSpawn(words)
			if err != nil {
				fmt.Println("Incorrect location or batallion type")
				continue
			}
		} else if words[0] == "move" {
			move, err := NewGameState.CommandMove(words)
			if err != nil {
				fmt.Println("Game is paused, you must wait for it to resume")
				continue
			}
			fmt.Println(move)
			exchange := routing.ExchangePerilTopic
			routingKey := routing.ArmyMovesPrefix + "." + username
			err = pubsub.PublishJSON(newConn, exchange, routingKey, move)
			if err != nil {
				log.Fatal(err)
			}
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) string {
	return func(ps routing.PlayingState) string {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return "Ack"
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) string {
	return func(ms gamelogic.ArmyMove) string {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(ms)
		if outcome == 1 || outcome == 2 {
			return "Ack"
		} else {
			return "NackDiscard"
		}
	}
}

// queueType is 1 if it is durable, it is 2 if it is transient
func CreateAndBind(Type string, username string, conn *amqp.Connection, gamestate *gamelogic.GameState) error {
	if Type == "pause" {
		exchange := routing.ExchangePerilDirect
		queueName := routing.PauseKey + "." + username
		routingKey := routing.PauseKey
		queueType := 2
		err := pubsub.SubscribeJSON(conn, exchange, queueName, routingKey, pubsub.SimpleQueueType(queueType), handlerPause(gamestate))
		if err != nil {
			return err
		}
	} else if Type == "army" {
		exchange := routing.ExchangePerilTopic
		queueName := routing.ArmyMovesPrefix + "." + username
		routingKey := routing.ArmyMovesPrefix + ".*"
		queueType := 2
		err := pubsub.SubscribeJSON(conn, exchange, queueName, routingKey, pubsub.SimpleQueueType(queueType), handlerMove(gamestate))
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Unknown type")
	}
	return nil 
}
