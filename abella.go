package main

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Canal per enviar missatges per a que l'abella produeixi mel
	beeProduceHoney, err := ch.QueueDeclare(
		"produce", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Canal per a despertar l'ós
	wakeBearUp, err := ch.QueueDeclare(
		"wakeUp", // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Per rebre les indicacions de l'ós per produïr mel
	msgsPermit, err := ch.Consume(
		beeProduceHoney.Name, // queue
		"",                   // consumer
		true,                 // auto-ack
		false,                // exclusive
		false,                // no-local
		false,                // no-wait
		nil,                  // args
	)
	failOnError(err, "Failed to register a consumer")

	// ** Missatges de finalització **
	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	qStop, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		qStop.Name, // queue name
		"",         // routing key
		"logs",     // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		qStop.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")
	// ***************************

	forever := make(chan bool)
	body := os.Args[1] // bee name

	go func() {
		for d := range msgs {
			if string(d.Body) == "END" {
				log.Printf("The pot is broken. %s can no longer fill it up!", body)
				log.Printf("%s says bye", body)
				log.Printf("SIMULATION ENDS")
				os.Exit(0)
			}
		}
	}()

	go func() {
		for d := range msgsPermit {
			honeyDose, _ := strconv.Atoi(string(d.Body)) // per a poder fer la comparació al if
			if honeyDose > 0 {                           // per evitar missatge honey 0
				log.Printf("The bee %s is producing honey: %d", body, honeyDose)

				rand.Seed(time.Now().UnixNano())
				n := rand.Intn(3000) // n will be between 0 and 10
				time.Sleep(time.Duration(n) * time.Millisecond)

				if honeyDose == 10 {
					log.Printf("The bee %s got the bear up", body)
					err = ch.Publish(
						"",              // exchange
						wakeBearUp.Name, // routing key
						false,           // mandatory
						false,           // immediate
						amqp.Publishing{
							DeliveryMode: amqp.Persistent,
							ContentType:  "text/plain",
							Body:         []byte(body),
						})
					failOnError(err, "Failed to publish a message")

				}
			}
		}
	}()
	log.Printf(" [*] This is the bee %s", body)
	<-forever

}
