package database

import (
	"fmt"
	"goredis/datastruct/dict"
	"goredis/interface/database"
	"goredis/interface/redis"
	"goredis/lib/timewheel"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

type DB struct {
	index      int
	dict       *dict.ConcurrentDict
	ttlMap     *dict.ConcurrentDict
	versionMap *dict.ConcurrentDict

	addAof func(CmdLine)
}

type ExecFunc func(db *DB, args [][]byte) redis.Reply

type PreFunc func(args [][]byte) ([]string, []string)

type CmdLine = [][]byte

type UndoFunc func(db *DB, args [][]byte) []CmdLine

func newDB() *DB {
	return &DB{
		dict:       dict.NewConcurrent(dataDictSize),
		ttlMap:     dict.NewConcurrent(ttlDictSize),
		versionMap: dict.NewConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
}

func newBasicDB() *DB {
	db := &DB{
		dict:       dict.NewConcurrent(dataDictSize),
		ttlMap:     dict.NewConcurrent(ttlDictSize),
		versionMap: dict.NewConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

func (db *DB) Exec(c redis.Conn, cmdLine [][]byte) redis.Reply {

}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {

}

func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {

}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

// GetEntity 判断key是否过期 TODO 线程安全？
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.dict.GetWithLock(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// TODO where is lock?
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.dict.PutWithLock(key, entity)
}

// TODO where is lock?
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.dict.PutIfExistsWithLock(key, entity)
}

// TODO where is lock?
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.dict.PutIfAbsentWithLock(key, entity)
}

// Remove 删除Key 需要持锁
func (db *DB) Remove(key string) {
	db.dict.RemoveWithLock(key)
	db.ttlMap.Remove(key)
	taskKey :=
}

// Removes 移除传入的keys，不需要持锁
func (db *DB) Removes(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		_, exists := db.dict.GetWithLock(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush() {
	db.dict.Clear()
	db.ttlMap.Clear()
}

func (db *DB) RWLocks(writeKeys, readkeys []string) {
	db.dict.RWLocks(writeKeys, readkeys)
}

func (db *DB) RWUnlock(writeKeys, readkeys []string) {
	db.dict.RWUnLocks(writeKeys, readkeys)
}

func genExpireTask(key string) string {
	return fmt.Sprintf("expire:%s", key)
}

// Expire 对key设置过期时间
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnlock(keys, nil)
		rawExpire, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		expire, _ := rawExpire.(time.Time)
		expired := time.Now().After(expire)
		if expired {
			db.Remove(key)
		}
	})
}

// Persister 取消key的过期时间
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	timewheel.Cancel(genExpireTask(key)
}

func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		// key如果过期了被删除了，那么ttlmap中就不存在了
		return true
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

func (db *DB) addVersion(keys ...string) {
	for _, k := range keys {
		versionCode := db.GetVersion(k)
		db.versionMap.Put(k, versionCode+1)
	}
}

func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

func (db *DB) ForEach(consumer func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.dict.ForEach(func(key string, v interface{}) bool {
		entity, _ := v.(*database.DataEntity)

		var expiration *time.Time
		rawExpiration, ok := db.ttlMap.Get(key)

	}
}
