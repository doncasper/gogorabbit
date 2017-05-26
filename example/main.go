package main

import (
	"fmt"
	"log"
	"time"

	"github.com/doncasper/gogorabbit"
	"github.com/spf13/viper"
)

func consumerHandler(data []byte) error {
	log.Printf("[CONSUMED] %s\n", data)

	return nil
}

func runRabbitMQ(config *viper.Viper) {
	rabbit, err := gogorabbit.New(config)
	if err != nil {
		log.Fatal(err)
	}

	if err := rabbit.SetExchange(config.Sub("exchanges.test_exchange")); err != nil {
		log.Fatal(err)
	}

	if err := rabbit.SetQueue(config.Sub("queues.test_queue")); err != nil {
		log.Fatal(err)
	}

	if err := rabbit.RunConsumer(config.Sub("consumers.test_consumer"), consumerHandler); err != nil {
		log.Fatal(err)
	}

	if err := rabbit.RegisterProducer(config.Sub("producers.test_producer")); err != nil {
		log.Fatal(err)
	}

	p, ok := rabbit.GetProducer("test-producer")
	if !ok {
		log.Fatal("Cant find producer!")
	}

	go func() {
		for {
			msg := fmt.Sprintf("Current TS: %d", time.Now().Unix())

			log.Printf("[PRODUCED] %s\n", msg)
			p.Produce([]byte(msg))
			time.Sleep(time.Millisecond * 100)
		}
	}()
}

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	runRabbitMQ(viper.Sub("mq"))

	// Run pseudo server
	select {}
}
