package tcp

import (
	"context"
	"errors"
	"fmt"
	"goredis/interface/tcp"
	"goredis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Config struct {
	Addr    string        `yaml:"addr"`
	MaxConn uint32        `yaml:"maxConn"`
	Timeout time.Duration `yaml:"timeout"`
}

var ClientCounter int32

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan, signalChan := make(chan struct{}), make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGHUP)
	go func() {
		sig := <-signalChan
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGHUP:
			closeChan <- struct{}{}
		}
	}()

	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind:%s, start listening...", cfg.Addr))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(listen net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	errChan := make(chan error, 1)
	defer close(errChan)
	go func() {
		select {
		case <-closeChan:
			logger.Info(fmt.Sprintf("go exit signal"))
		case err := <-errChan:
			logger.Info(fmt.Sprintf("accept error:%s", err))
		}
		logger.Info("shutting down...")
		_ = listen.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listen.Accept()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Temporary() {
				// 网络抖动等暂时性错误，等待一段时间后重试即可
				logger.Infof("accept occurs temporary error: %v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			errChan <- err
			break
		}

		// handle
		logger.Info("accept link")
		ClientCounter++
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
				atomic.AddInt32(&ClientCounter, -1)
			}()
			handler.Handle(ctx, conn)
		}()
		waitDone.Wait()
	}
}
