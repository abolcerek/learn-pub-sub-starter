package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable = iota + 1
	Transient
)

func PublishJSON[T any](ch * amqp.Channel, exchange, key string, val T) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ctx := context.Background()
	mandatory := false
	immediate := false
	pubStruct := amqp.Publishing{
		ContentType: "application/json",
		Body: bytes,
	}
	err = ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, pubStruct)
	if err != nil {
		return err
	}
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange string, queueName string, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	newConn, err := conn.Channel()
	if err != nil{
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	var durable bool
	var autoDelete bool
	var exclusive bool
	if queueType == 1 {
		durable = true
		autoDelete = false
		exclusive = false
	}
	if queueType == 2 {
		durable = false
		autoDelete = true
		exclusive = true
	}
	queue, err := newConn.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	err = newConn.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	return newConn, queue, nil
}

