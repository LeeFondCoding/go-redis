package client

import (
	"goredis/interface/redis"
	"goredis/lib/logger"
	"goredis/lib/sync/wait"
	"goredis/redis/parser"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	running
	closed
)

type Client struct {
	conn        net.Conn
	pendingReqs chan *request
	waitingReqs chan *request
	addr        string

	status int32
	// 代表还未发送的响应
	working *sync.WaitGroup
}

type request struct {
	id    uint64
	args  [][]byte
	reply redis.Reply
	// ????
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		addr:        addr,
		working:     &sync.WaitGroup{},
	}, nil
}

func (c *Client) Start() {

}

func (c *Client) Close() error {

}

func (c *Client) reconnect() {}

func (c Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
}

// finishRequest 处理客户端的请求结束逻辑，确保请求被正确处理并通知等待的goroutine
func (c *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()

	request := <-c.waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}

// handleRead 开一个协程读取并解析网络连接中数据，使用chan传递
func (c *Client) handleRead() {
	ch := parser.ParseStream(c.conn)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&c.status)
			if status == closed {
				return
			}
			c.reconnect()
			return
		}
		c.finishRequest(payload.Data)
	}
}
