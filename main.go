package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Azure/go-amqp"
)

var ADDR string = "localhost:1883"
var TOPIC string = "topicName/#"

func main() {
	ctx := context.Background()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	b := connectToBroker(ctx, ADDR)
	b.newSession(ctx)
	r := b.newReceiver(ctx, TOPIC)

	go func() {
		for {
			msg, err := r.Receive(ctx, &amqp.ReceiveOptions{})
			if err != nil {
				slog.Error("error receiving message: ", slog.String("error", err.Error()))
				break
			}

			slog.Info("received message:", slog.String("data", string(msg.GetData())))
		}
	}()

	<-sigChan
	b.closeConnections(ctx)
}
