// Package aof
//
// 完成了各种数据结构的value的序列化
package aof

import (
	"goredis/datastruct/dict"
	"goredis/datastruct/list"
	"goredis/datastruct/set"
	"goredis/datastruct/sortedset"
	"goredis/interface/database"
	"goredis/redis/protocol"
	"strconv"
	"time"
)

func EntityToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var reply *protocol.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		reply = stringToCmd(key, val)
	case list.List:
		reply = listToCmd(key, val)
	case set.Set:
		reply = setToCmd(key, val)
	case dict.Dict:
		reply = hashToCmd(key, val)
	case *sortedset.SortedSet:
		reply = zSetToCmd(key, val)
	}
	return reply
}

var setCmd = []byte("SET")

func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return protocol.MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSH")

func listToCmd(key string, list list.List) *protocol.MultiBulkReply {
	args := make([][]byte, 2, 2+list.Len())
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	list.ForEach(func(i int, val interface{}) bool {
		args = append(args, val.([]byte))
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var hMSetCmd = []byte("HMSET")

func hashToCmd(key string, hash dict.Dict) *protocol.MultiBulkReply {
	// cmd + key + hashLen * (1field + 1value)
	args := make([][]byte, 2, 2+hash.Len()*2)
	args[0], args[1] = hMSetCmd, []byte(key)
	hash.ForEach(func(key string, val interface{}) bool {
		args = append(args, []byte(key), val.([]byte))
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var sAddCmd = []byte("SADD")

func setToCmd(key string, set set.Set) *protocol.MultiBulkReply {
	args := make([][]byte, 2, set.Len()+2)
	args[0] = sAddCmd
	args[1] = []byte(key)
	set.ForEach(func(elem string) bool {
		args = append(args, []byte(elem))
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var zAddCmd = []byte("ZADD")

func zSetToCmd(key string, zset *sortedset.SortedSet) *protocol.MultiBulkReply {
	args := make([][]byte, 2, zset.Len()*2+2)
	args[0] = zAddCmd
	args[1] = []byte(key)
	zset.ForEach(0, zset.Len(), true, func(elem *sortedset.Element) bool {
		args = append(args, []byte(strconv.FormatFloat(elem.Score, 'f', -1, 64)), []byte(elem.Member))
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var pExpireAtBytes = []byte("PEXPIREAT")

func MakeExpireCmd(key string, expireTime time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireTime.UnixNano()/1e6, 10))
	return protocol.MakeMultiBulkReply(args)
}
