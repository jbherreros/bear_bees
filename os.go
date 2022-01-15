// Juan Carlos Bujosa Herreros and José Luis García Herreros
package main

import (
	"log"
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

	// Missatge de finalització a totes les abelles
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

	// Reb el missatge que desperta l'ós
	msgs, err := ch.Consume(
		wakeBearUp.Name, // queue
		"",              // consumer
		true,            // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	failOnError(err, "Failed to register a consumer")

	// D'inici, l'ós demana a les abelles que produeixin mel
	potSize := 10
	for i := 1; i <= potSize; i++ {
		err = ch.Publish(
			"",                   // exchange
			beeProduceHoney.Name, // routing key
			false,                // mandatory
			false,                // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(strconv.Itoa(i)),
			})
		failOnError(err, "Failed to publish a message")
	}

	forever := make(chan bool)
	potBreak := 3
	it := 1
	go func() {
		for d := range msgs {
			// L'ós es menja el pot
			log.Printf("The bear is awake. The bee %s got him up. He is eating...", d.Body)
			time.Sleep(3000 * time.Millisecond)
			log.Printf("The bear goes to bed")

			if it < potBreak {
				// Demanam a les abelles que produeixin més mel
				for i := 1; i <= potSize; i++ {
					err = ch.Publish(
						"",                   // exchange
						beeProduceHoney.Name, // routing key
						false,                // mandatory
						false,                // immediate
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        []byte(strconv.Itoa(i)),
						})
					failOnError(err, "Failed to publish a message")
				}

			} else {
				log.Printf("\nThe bear fed up with the honey. He broke up the pot!")

				// Envia un missatge a totes les abelles per finalitzar la seva execució
				body := "END"
				err = ch.Publish(
					"logs", // exchange
					"",     // routing key
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(body),
					})
				failOnError(err, "Failed to publish a message")

				log.Printf("SIMULATION ENDS")
				os.Exit(0)
			}
			it++
		}
	}()

	log.Printf(" [*] The bear sleeps while is not given honey")
	<-forever
}
