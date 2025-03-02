package tcp

import (
	"bufio"
	"context"
	"goredis/lib/logger"
	"goredis/lib/sync/atomic"
	"goredis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

type EchoClient struct {
	conn    net.Conn
	waiting wait.Wait
}

func (c *EchoClient) Close() error {
	c.waiting.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}

	client := &EchoClient{conn: conn}
	h.activeConn.Store(client, struct{}{})

	reader := bufio.NewReader(conn)
	for {
		// may occur: client EOF, client timeout, server early close
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection closed")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.waiting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.waiting.Done()
	}
}

func (h *EchoHandler) Close() error {
	logger.Info("handle shutting down")
	h.closing.Set(true)
	h.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Close()
		return true
	})
	return nil
}
