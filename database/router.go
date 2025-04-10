package database

import "strings"

var cmdTable = make(map[string]*command)

type command struct {
	name     string
	executor ExecFunc

	prepare PreFunc

	undo UndoFunc

	arity int
	flags int
	extra *commandExtra
}

type commandExtra struct {
	signs    []string
	firstKey int
	lastKey  int
	keyStep  int
}

const (
	flagWrite = iota
	flagReadOnly
	flagSpecial
)

func registerCommand(name string, executor ExecFunc, prepare PreFunc, undo UndoFunc, arity, flags int) *command {
	name = strings.ToLower(name)
	cmd := &command{
		name:     name,
		executor: executor,
		prepare:  prepare,
		undo:     undo,
		arity:    arity,
		flags:    flags,
	}
	cmdTable[name] = cmd
	return cmd
}
