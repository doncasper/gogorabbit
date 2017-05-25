package gogorabbit

import (
	"fmt"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
	"github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

type ConsumerHandler func([]byte) error

type DoneChannel chan struct{}

type RabbitMQ struct {
	exchanges
	connection wabbit.Conn
	channel    wabbit.Channel
	logger     logrus.FieldLogger
}

func New(dsn string) (*RabbitMQ, error) {
	var err error

	rabbit := &RabbitMQ{
		exchanges: make(exchanges),
		logger:    logrus.WithField("logger", "gogorabbit"),
	}

	rabbit.logger.Debug("Start dialing connection to RabbitMQ!")
	rabbit.connection, err = amqp.Dial(dsn)
	if err != nil {
		return nil, fmt.Errorf("Error dialing RabbitMQ connection: %s", err)
	}

	go func() {
		// TODO: Handle the situation when the connection is closed. Reconnection?
		rabbit.logger.Warnf("Ooops! RabbitMQ connection is closed: %s", <-rabbit.connection.NotifyClose(make(chan wabbit.Error)))
	}()

	rabbit.channel, err = rabbit.NewChannel()
	if err != nil {
		return nil, fmt.Errorf("Cant get new channel for RabbitMQ connection: %s", err)
	}

	if err := rabbit.channel.Qos(1, 0, false); err != nil {
		return nil, err
	}

	return rabbit, nil
}

func (rabbit *RabbitMQ) NewChannel() (wabbit.Channel, error) {
	rabbit.logger.Debug("Get channel for RabbitMQ connection!")
	return rabbit.connection.Channel()
}

func (rabbit *RabbitMQ) SetExchange(config *viper.Viper) (err error) {
	rabbit.logger.Debugf("Start setting exchange: %s", config.GetString("name"))

	if _, ok := rabbit.GetExchange(config.GetString("name")); ok {
		return fmt.Errorf("Rabbit is already connected to %s exchange.", config.GetString("name"))
	}

	exchange := &exchange{
		Name:   config.GetString("name"),
		Type:   config.GetString("type"),
		queues: make(queues),
	}

	exchange.SetOptions(config.GetStringMap("options"))

	if err = rabbit.channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		exchange.Options(),
	); err != nil {
		return fmt.Errorf("Can't decalare exchange: %s", err)
	}

	rabbit.AddExchange(exchange)

	return
}

func (rabbit *RabbitMQ) SetQueue(config *viper.Viper) (err error) {
	exchangeName := config.GetString("exchange")
	exchange, ok := rabbit.exchanges[exchangeName]
	if !ok {
		return fmt.Errorf("You should set up exchange, before bind queue to %s exchange.", exchangeName)
	}

	queueName := config.GetString("name")
	if _, ok := rabbit.exchanges[exchangeName].queues[queueName]; ok {
		return fmt.Errorf("Queue %s is already binded to %s exchange.", queueName, config.GetString("exchange"))
	}

	rabbit.logger.Debugf("Start setting queue %s for exchange: %s", queueName, exchangeName)

	queue := &queue{
		Name:         queueName,
		ExchangeName: exchangeName,
		BindKey:      config.GetString("bind_key"),
		consumers:    make(consumers),
	}

	queue.SetOptions(config.GetStringMap("options"))

	rabbit.logger.Debugf("Declare queue %s in RabbitMQ channel!", queue.Name)
	_, err = rabbit.channel.QueueDeclare(
		queue.Name,
		queue.Options(),
	)
	if err != nil {
		return fmt.Errorf("Cant declare queue: %s (%s)", queue.Name, err)
	}

	rabbit.logger.Debugf("Bind queue %s to RabbitMQ channel!", queue.Name)
	if err = rabbit.channel.QueueBind(
		queue.Name,
		queue.BindKey,
		queue.ExchangeName,
		queue.Options(),
	); err != nil {
		return fmt.Errorf("Queue Bind: %s", err)
	}

	exchange.AddQueue(queue)

	return
}

func (rabbit *RabbitMQ) RunConsumer(config *viper.Viper, handler ConsumerHandler) (err error) {
	consumer := &consumer{
		Name:         config.GetString("name"),
		ExchangeName: config.GetString("exchange"),
		QueueName:    config.GetString("queue"),
		Handler:      handler,
	}

	consumer.SetOptions(config.GetStringMap("options"))

	exchange, ok := rabbit.GetExchange(consumer.ExchangeName)
	if !ok {
		err = fmt.Errorf("The exchange: %s is not yet initialized!", consumer.ExchangeName)

		return
	}

	queue, ok := exchange.GetQueue(consumer.QueueName)
	if !ok {
		err = fmt.Errorf("The queue: %s is not yet initialized for exchange: %s", consumer.QueueName, consumer.ExchangeName)

		return
	}

	if _, ok := queue.GetConsumer(consumer.QueueName); ok {
		err = fmt.Errorf("Consumer: %s is already runned for %s queue.", consumer.Name, queue.Name)

		return
	}

	for i := 1; i <= config.GetInt("workers"); i++ {
		worker := &consumerWorker{
			Tag:  fmt.Sprintf("%s_%d", consumer.Name, i),
			Done: make(chan struct{}, 1),
		}
		worker.logger = rabbit.logger.WithField("logger", "consumer."+worker.Tag)

		if err = consumer.runWorker(rabbit.channel, worker); err != nil {
			return
		}

		consumer.AddWorker(worker)
	}

	queue.AddConsumer(consumer)

	return
}

var globalRabbit *RabbitMQ

// InitGlobal initializes the connection to the RabbitMQ in a global variable `MQ`
// so that you can use it anywhere in your application. But, I think this is a bad
// practice, so avoid global variables if you have the opportunity.
func InitGlobal(dsn string) (err error) {
	globalRabbit, err = New(dsn)
	if err != nil {
		return err
	}

	return nil
}

// MQ return link to global RabbitMQ object. Ensure that before calling this function,
// you initialized the connection globally with calling of `InitGlobal` function.
func MQ() *RabbitMQ {
	return globalRabbit
}
