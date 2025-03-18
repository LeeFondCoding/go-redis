package redis

// Conn 代表与Redis客户端的连接
type Conn interface {
	Write([]byte) (int, error)
	Close() error

	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueueCmds()
	GetWatching() map[string]uint32
	AddTxError(error)
	GetTxErrors() []error

	GetDBIndex() int
	SelectDB(int)

	Name() string
}
