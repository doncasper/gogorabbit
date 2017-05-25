package main

import (
	"log"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/doncasper/gogorabbit"
	"github.com/spf13/viper"
)

const dsn = "amqp://guest:guest@localhost:5672/"

func consumerHandler(data []byte) error {
	id := time.Now().UnixNano()
	log.Printf("[%d] Started processing: %s\n", id, data)

	// Some log message processing:
	time.Sleep(time.Second * 5)

	log.Printf("[%d] Finished processing: %s\n", id, data)

	return nil
}

func runMQ(config *viper.Viper) {
	rabbit, err := gogorabbit.New(config.GetString("dsn"))
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

	//rabbit.RunProducer(config.Sub("producer.stats"))
}

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	viper.Set("mq", map[string]interface{}{
		"dsn":                dsn,
		"reconnection_tries": 5,
		"reconnection_delay": 5, // Seconds
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
	})

	runMQ(viper.Sub("mq"))

	// Run pseudo server
	select {}
}
