package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/doncasper/gogorabbit"
	"github.com/spf13/viper"
)

type Message struct {
	Body      string `json:"msg"`
	Producer  string `json:"producer"`
	Timestamp int64  `json:"ts"`
}

func consumerHandler(delivery gogorabbit.Delivery, sender gogorabbit.ErrorSender) {
	log.Printf("<<< %s: %s\n", delivery.ConsumerTag(), delivery.Body())

	var message Message

	if err := json.Unmarshal(delivery.Body(), &message); err != nil {
		// We can send errors for logging.
		sender.SendError(err)

		// We do not want to return the broken message to the
		// queue, so we pass the argument `requeue` as `false`.
		delivery.Nack(false, false)

		return
	}

	log.Printf("--- Processed message: %s from %s at %d", message.Body, message.Producer, message.Timestamp)

	delivery.Ack(false)
}

func runRabbitMQ(config *viper.Viper) {
	rabbit, err := gogorabbit.New(config.GetString("dsn"), config.GetDuration("reconnect_delay")*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	// Log RabbitMQ errors.
	go func() {
		for err := range rabbit.Errors() {
			log.Println(err)
		}
	}()

	if err := rabbit.SetExchange(config.GetStringMap("exchanges.test_exchange")); err != nil {
		log.Fatal(err)
	}

	if err := rabbit.SetQueue(config.GetStringMap("queues.test_queue")); err != nil {
		log.Fatal(err)
	}

	if err := rabbit.RunConsumer(config.GetStringMap("consumers.test_consumer"), consumerHandler); err != nil {
		log.Fatal(err)
	}

	if err := rabbit.RegisterProducer(config.GetStringMap("producers.test_producer")); err != nil {
		log.Fatal(err)
	}

	p, ok := rabbit.GetProducer("test-producer")
	if !ok {
		log.Fatal("Cant find producer!")
	}

	go func(producer gogorabbit.Producer) {
		for {
			msg := fmt.Sprintf(`{"msg": "Hello!", "producer": "%s", "ts": %d}`, producer.Name(), time.Now().Unix())

			log.Println(">>>", msg)
			producer.Produce([]byte(msg))

			time.Sleep(time.Millisecond * 500)
		}
	}(p)
}

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	runRabbitMQ(viper.Sub("mq"))

	// Run pseudo server.
	select {}
}
