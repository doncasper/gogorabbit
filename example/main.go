package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/doncasper/gogorabbit"
	"github.com/spf13/viper"
)

const dsn = "amqp://guest:guest@localhost:5672/"

func consumerHandler(data []byte) error {
	log.Printf("[CONSUMED] %s\n", data)

	return nil
}

func runMQ(config *viper.Viper) {
	rabbit, err := gogorabbit.New(config)
	if err != nil {
		log.Fatal(err)
	}

	if err := rabbit.SetExchange(config.Sub("exchangers.test_exchange")); err != nil {
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
	logrus.SetLevel(logrus.DebugLevel)

	// TODO: Move to config file!
	viper.Set("mq", map[string]interface{}{
		"dsn":                dsn,
		"reconnection_delay": 2, // Seconds
		"exchangers": map[string]interface{}{
			"test_exchange": map[string]interface{}{
				"name": "test-exchange",
				"type": "direct",
				"options": map[string]interface{}{
					"durable":  true,
					"delete":   false,
					"internal": false,
					"no_wait":  false,
				},
			},
		},
		"queues": map[string]interface{}{
			"test_queue": map[string]interface{}{
				"name":     "test-queue",
				"exchange": "test-exchange",
				"bind_key": "test-key",
				"options": map[string]interface{}{
					"durable":   true,
					"delete":    false,
					"exclusive": false,
					"no_wait":   false,
				},
			},
		},
		"consumers": map[string]interface{}{
			"test_consumer": map[string]interface{}{
				"name":     "test-consumer",
				"workers":  3,
				"queue":    "test-queue",
				"exchange": "test-exchange",
				"options": map[string]interface{}{
					"no_ack":    false,
					"exclusive": false,
					"no_local":  false,
					"no_wait":   false,
				},
			},
		},
		"producers": map[string]interface{}{
			"test_producer": map[string]interface{}{
				"name":        "test-producer",
				"exchange":    "test-exchange",
				"routing_key": "test-key",
				"buffer_size": 10,
				"options": map[string]interface{}{
					"deliveryMode": 2, // Persistent
				},
			},
		},
	})

	runMQ(viper.Sub("mq"))

	// Run pseudo server
	select {}
}
