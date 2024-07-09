package main

import (
	"context"
	"log/slog"

	"github.com/Azure/go-amqp"
)

type broker struct {
	conn     *amqp.Conn
	session  *amqp.Session
	receiver *amqp.Receiver
}

func connectToBroker(ctx context.Context, addr string) *broker {
	conn, err := amqp.Dial(ctx, addr, &amqp.ConnOptions{})
	if err != nil {
		slog.Error("Failed to connect amqp broker", slog.String("addr", addr))
	}

	return &broker{
		conn: conn,
	}
}

func (b *broker) newSession(ctx context.Context) *amqp.Session {
	s, err := b.conn.NewSession(ctx, &amqp.SessionOptions{})
	if err != nil {
		slog.Error("Failed to create session", "error", err)
		return nil
	}
	b.session = s

	return s
}

func (b *broker) newReceiver(ctx context.Context, addr string) *amqp.Receiver {
	r, err := b.session.NewReceiver(ctx, addr, &amqp.ReceiverOptions{})
	if err != nil {
		slog.Error("Failed to create receiver", "addr", addr)
	}

	b.receiver = r
	return r
}

func (b *broker) closeConnections(ctx context.Context) error {
	if err := b.conn.Close(); err != nil {
		return err
	}

	if err := b.session.Close(ctx); err != nil {
		return err
	}

	if err := b.receiver.Close(ctx); err != nil {
		return err
	}

	return nil
}
