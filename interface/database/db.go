package database

import (
	"github.com/hdt3213/rdb/core"
	"goredis/interface/redis"
	"time"
)

// CmdLine [][]byte的别名. 代表命令行数据
type CmdLine = [][]byte

// DB 是一个Redis类型的存储引擎
type DB interface {
	Exec(conn redis.Conn, line CmdLine) redis.Reply
	AfterClientClose(c redis.Conn)
	Close()
	LoadRDB(dec *core.Decoder) error
}

// DBEngine 是一个嵌入的存储引擎，为了更多的应用，暴露了更多的方法
type DBEngine interface {
	DB
	ExecWithLock(conn redis.Conn, line CmdLine) redis.Reply
	ExecMulti(conn redis.Conn, watching map[string]uint32, cmdLines []CmdLine) redis.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnlocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
}

// DataEntity 储存了key映射的数据，包括string, list, hash, set, zset 等等
type DataEntity struct {
	Data any
}
