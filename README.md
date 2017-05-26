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

## Reconnection

Base delay for reconnection you define when create new connection to RabbitMQ.

After each failed attempt to establish a connection to the RabbitMQ, reconnect delay will be increased on 15%.

## Handlers

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
```

## Logging

The easiest way to logging all errors it's running new goroutine which will be handle all errors:
```go
rabbit, _ := gogorabbit.New(dsn, reconnectionDelay)

go func() {
    for err := range rabbit.Errors() {
        log.Println(err)
    }
}()
```