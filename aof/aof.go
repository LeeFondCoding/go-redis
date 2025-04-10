package aof

import (
	"context"
	"goredis/interface/database"
	"goredis/lib/logger"
	"os"
	"strings"
	"sync"
	"time"
)

type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16 // 64k
)

const (
	// FsyncAlways do fsync for every command
	FsyncAlways = "always"
	// FsyncEverySec do fsync every second
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

type Listener interface {
	Callback([]CmdLine)
}

type Persister struct {
	ctx        context.Context
	cancel     context.CancelFunc
	db         database.DBEngine
	tmpDBMaker func() database.DBEngine

	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	aofFsync    string
	aofFinished chan struct{}
	pausingAof  sync.Mutex
	currentDB   int
	listeners   map[Listener]struct{}
	buffer      []CmdLine
}

func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	p := &Persister{
		db:          db,
		tmpDBMaker:  tmpDBMaker,
		aofChan: make(chan *payload, aofQueueSize),
		aofFilename: filename,
		aofFsync:    strings.ToLower(fsync),
		aofFinished: make(chan struct{}),
		pausingAof:  sync.Mutex{},
		listeners:   make(map[Listener]struct{}),
	}
	if load {
		p.
	}
}

func (p *Persister) RemoveListener(listener Listener) {
	p.pausingAof.Lock()
	defer p.pausingAof.Unlock()
	delete(p.listeners, listener)
}

func (p *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	if p.aofChan == nil {
		return
	}

	if p.aofFsync == FsyncAlways {
		p := &payload{
			dbIndex: dbIndex,
			cmdLine: cmdLine,
		}
		p.write
	}
}

func (p *Persister) listenCmd() {
	for p := range p.aofChan {
		p.
	}
}

func (p *Persister) writeAof(p *payload) {
	p.buffer = p.buffer[:0]
}

func (p *Persister) Fsync() {
	p.pausingAof.Lock()
	defer p.pausingAof.Unlock()
	if err := p.aofFile.Sync(); err != nil {
		logger.Errorf("fsync failed: %v", err)
	}
}

func (p *Persister) Close() {
	if p.aofFile != nil {
		close(p.aofChan)
	    <-p.aofFinished
		err := p.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	p.cancel()
}

func (p *Persister) fsyncEverySecond() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:

			case <-p.ctx.Done():
				return
			}
		}
	}()
}

func (p *Persister) generateAof(ctx context.Context) {
	tmpFile := ctx.tmpFile
}
