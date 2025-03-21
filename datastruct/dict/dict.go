package dict

type Consumer func(key string, val any) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (result int)
	ForEach(consumer Consumer)
	Keys() []string
	// RandomKeys 随机返回limit个key
	RandomKeys(limit int) []string
	// RandomDistinctKeys 随机返回limit个不重复的key
	RandomDistinctKeys(limit int) []string
	Clear()
}
