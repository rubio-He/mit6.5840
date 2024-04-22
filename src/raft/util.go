package raft

import (
	"fmt"
	"log"
)

// Debugging
const DebugLevel = STATE | EVENT | WARN

type Topic int

const (
	RPC   Topic = 0b1
	INFO  Topic = 0b10
	STATE Topic = 0b100
	EVENT Topic = 0b1000
	WARN  Topic = 0b10000

	VOTING          Topic = 0b10000000
	LOG_REPLICATING Topic = 0b100000000
)

func (rf *Raft) debug(dLvl Topic, str string, a ...any) {
	if dLvl&DebugLevel != 0 {
		str = str + "\n"
		prefix := fmt.Sprintf("SEVER(%d): ", rf.me)
		log.Printf(prefix+str, a...)
	}
}

func (rf *Raft) debugState() {
	if STATE&DebugLevel != 0 {
		str := fmt.Sprintf("Raft: %+v\n", *rf)
		prefix := fmt.Sprintf("SEVER(%d): ", rf.me)
		log.Printf(prefix + str)
	}
}

func logTermAt(log *[]Log, idx int) int {
	if idx == 0 {
		return INITIAL_TERM
	}
	if idx >= len(*log) {
		panic(fmt.Sprintf("Index out of boundary: %d, Slice length is %d", idx, len(*log)))
	}
	return (*log)[idx].Term
}

func lastEntry(log *[]Log) *Log {
	return &(*log)[len(*log)-1]
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
