Go Go Rabbit!!!
===============

It's a simple client for RabbitMQ with all what you need.

## Installation

```shell
$ go get github.com/doncasper/gogorabbit
```

## Dependencies

```shell
$ go get github.com/NeowayLabs/wabbit
```

## Example

You can see full example in [example folder](https://github.com/doncasper/gogorabbit/tree/master/example).

## Connect to RabbitMQ

You can create local instance of RabbitMQ client: 

```go
func main() {
    dsn := "amqp://guest:guest@localhost:5672/"
    reconnectDelay := time.Second * 2
    
    rabbit, err := gogorabbit.New(dsn, reconnectDelay)
    if err != nil {
        log.Fatal(err)
    }
    
    defer rabbit.Close()
}

```

Or you can init connection globally:

```go
func main() {
    dsn := "amqp://guest:guest@localhost:5672/"
    reconnectDelay := time.Second * 2
    
    if err := gogorabbit.InitGlobal(dsn, reconnectDelay); err != nil {
        log.Fatal(err)
    }
    
    defer gogorabbit.MQ().Close()
}
```

After global init of connection you can get client object by global function of package:

```go
rabbit := gogorabbit.MQ()
```

## Logging

If you need log all errors from package use `Errors()` method for getting error channel.

The easiest way to logging all errors it's running new goroutine which will be handle all errors:
```go
rabbit, _ := gogorabbit.New(dsn, reconnectionDelay)

go func() {
    for err := range rabbit.Errors() {
        log.Println(err)
    }
}()
```

## Create exchange

```go
config := map[string]interface{}{
    "name":    "test-exchange",
    "type":    "direct",
    "options": map[string]interface{}{
        "durable":  true,
        "delete":   false,
        "internal": false,
        "no_wait":  true,
    },
}

if err := rabbit.SetExchange(config); err != nil {
    log.Fatal(err)
}
```

## Create queue

```go
config := map[string]interface{}{
    "name":     "test-queue",
    "exchange": "test-exchange",
    "bind_key": "test-key",
    "options": map[string]interface{}{
        "durable":   true,
        "delete":    false,
        "exclusive": false,
        "no_wait":   true,
    },
}

if err := rabbit.SetQueue(config); err != nil {
    log.Fatal(err)
}
```

## Run consumers

```go
func consumerHandler(delivery gogorabbit.Delivery, sender gogorabbit.ErrorSender) {
    var message Message
    
    if err := json.Unmarshal(delivery.Body(), &message); err != nil {
        // We can send errors for logging.
        sender.SendError(err)
        
        // We do not want to return the broken message to the
        // queue, so we pass the argument `requeue` as `false`.
        delivery.Nack(false, false)
        
        return
    }
    
    delivery.Ack(false)
}

config := map[string]interface{}{
    "name":     "test-consumer",
    "workers":  5,
    "exchange": "test-exchange",
    "queue":    "test-queue",
    "options": map[string]interface{}{
        "no_ack":    false,
        "no_local":  false,
        "exclusive": false,
        "no_wait":   false,
    },
}

if err := rabbit.RunConsumer(config, consumerHandler); err != nil {
    log.Fatal(err)
}
```

## Run producers
```go
config := map[string]interface{}{
    "name":        "test-producer",
    "exchange":    "test-exchange",
    "routing_key": "test-key",
    "buffer_size": 10,
    "options": map[string]interface{}{
        "delivery_mode": 2 // Persistent
    },
}

if err := rabbit.RegisterProducer(config); err != nil {
    log.Fatal(err)
}

p, ok := rabbit.GetProducer("test-producer")
if !ok {
    log.Fatal("Cant find producer!")
}

go func(producer gogorabbit.Producer) {
    for {
        producer.Produce([]byte("Hello from producer: " + producer.Name()))
        
        time.Sleep(time.Millisecond * 500)
    }
}(p)
```

## Reconnection

Base delay for reconnection you define when create new connection to RabbitMQ.

After each failed attempt to establish a connection to the RabbitMQ, reconnect delay will be increased on 15%.
