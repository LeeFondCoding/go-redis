package aof

import (
	"goredis/config"
	"goredis/lib/logger"
	"goredis/redis/protocol"
	"os"
	"strconv"
)

func (p *Persister) newRewriteHandler() *Persister {
	h :=&Persister{}
	h.aofFilename = p.aofFilename
	h.db = p.tmpDBMaker()
	return h
}

type RewriteCtx struct {
	tmpFile *os.File
	fileSize int64
	dbIdx int
}

func (p *Persister) Rewrite() error {
	ctx, err :=
}

func (p *Persister) StartRewrite() (*RewriteCtx, error) {
	p.pausingAof.Lock()
	defer p.pausingAof.Unlock()

	err := p.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	fileInfo, _ := os.Stat(p.aofFilename)
	filesize := fileInfo.Size()

	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}

	return &RewriteCtx{tmpFile: file, fileSize: filesize, dbIdx: 0}, nil
}

func (p *Persister) FinishRewrite(ctx *RewriteCtx) {
	p.pausingAof.Lock()
	defer p.pausingAof.Unlock()
	tmpFile := ctx.tmpFile

	errOccurs := func() bool {
		src, err := os.Open(p.aofFilename)
		if err != nil {
			logger.Error("open aof file failed: ", err.Error())
			return true
		}
		defer func() {
			_ = src.Close()
			_ = tmpFile.Close()
		} ()

		_, err = src.Seek(ctx.fileSize, 0)
		if err != nil {
			logger.Error("seek failed: ", err.Error())
			return true
		}

		data := protocol.MakeNullBulkReply("SELECT", strconv.Itoa(ctx.dbIdx)))
		_, err := tmpFile.Write(data)
		if err != nil {

		}
	}
}