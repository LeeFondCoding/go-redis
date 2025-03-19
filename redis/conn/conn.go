package conn

import (
	"goredis/lib/logger"
	"goredis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

const (
	flagSlave = uint64(1 << iota)
	flagMaster
	flagMulti
)

type Conn struct {
	conn net.Conn

	sendingData wait.Wait

	mu    sync.Mutex
	flags uint64

	subs map[string]bool

	password string

	// queued cmd for `multi`
	queue    [][][]byte
	watching map[string]uint32
	txErrors []error

	selectedDB int
}

var connPool = sync.Pool{
	New: func() interface{} { return &Conn{} },
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) Close() error {
	// 等待10秒或者数据处理完成关闭连接
	c.sendingData.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	connPool.Put(c)
	return nil
}

func NewConn(conn net.Conn) *Conn {
	c, ok := connPool.Get().(*Conn)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Conn{conn: conn}
	}
	c.conn = conn
	return c
}

func (c *Conn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	// 为什么godis要写个闭包呢
	defer c.sendingData.Done()
	return c.conn.Write(b)
}

func (c *Conn) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

func (c *Conn) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

func (c *Conn) UnSubsribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

func (c *Conn) SubsCount() int {
	return len(c.subs)
}

func (c *Conn) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, 0, len(c.subs))
	for channel := range c.subs {
		channels = append(channels, channel)
	}
	return channels
}

func (c *Conn) SetPassword(password string) {
	c.password = password
}

func (c *Conn) GetPassword() string {
	return c.password
}

func (c *Conn) InMultiState() bool {
	return c.flags&flagMulti > 0
}

func (c *Conn) SetMultiState(state bool) {
	if !state {
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti
		return
	}
	c.flags |= flagMulti
}

func (c *Conn) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

func (c *Conn) EnqueueCMd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

func (c *Conn) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

func (c *Conn) GetTxErrors() []error {
	return c.txErrors
}

// ClearQueuedCmds clears queued commands of current transaction
func (c *Conn) ClearQueuedCmds() {
	c.queue = nil
}

// GetWatching returns watching keys and their version code when started watching
func (c *Conn) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

// GetDBIndex returns selected db
func (c *Conn) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Conn) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}
