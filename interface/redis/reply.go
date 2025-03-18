package redis

// Reply redis序列化协议的接口
type Reply interface {
	ToBytes() []byte
}
