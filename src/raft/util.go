package raft

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"time"
)

// Debugging

const DebugLevel = STATE | VOTING

type Topic int

const (
	RPC   Topic = 0b1
	INFO  Topic = 0b10
	STATE Topic = 0b100
	EVENT Topic = 0b1000
	WARN  Topic = 0b10000

	VOTING          Topic = 0b10000000
	LOG_REPLICATING Topic = 0b100000000
	APPLY           Topic = 0b1000000000
	PERSIST         Topic = 0b10000000000
)

func (rf *Raft) debug(dLvl Topic, str string, a ...any) {
	pc, _, _, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	if dLvl&DebugLevel != 0 {
		str = str + "\n"
		prefix := fmt.Sprintf("SEVER(%d): [%s]: ", rf.me, funcName)
		log.Printf(prefix+str, a...)
	}
}

func (rf *Raft) debugState() {
	pc, _, _, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	if STATE&DebugLevel != 0 {
		str := fmt.Sprintf("Raft Log: %+v ", (*rf).log)
		str += fmt.Sprintf("Current Term: %d ", (*rf).currentTerm)
		str += fmt.Sprintf("Peer next Index: %+v ", (*rf).nextIndex)
		str += fmt.Sprintf("Commit Idx: %d ", (*rf).commitIndex)
		str += fmt.Sprintf("Last Applied: %d\n", (*rf).lastApplied)
		prefix := fmt.Sprintf("SEVER(%d): [%s]:", rf.me, funcName)
		log.Printf(prefix + str)
	}
}

func (rf *Raft) lastLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	}

	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) logAt(idx int) Log {
	if idx == 0 {
		panic("No LastEntryInRequest at idx 0")
	}

	if idx > len(rf.log) || idx < 0 {
		panic(fmt.Sprintf("Index out of boundary: %d, Slice length is %d", idx, len(rf.log)))
	}

	return rf.log[idx-1]
}

func (rf *Raft) logTermAt(idx int) int {
	if idx == 0 {
		return InitialTerm
	}
	if idx > len(rf.log) || idx < 0 {
		panic(fmt.Sprintf("Index out of boundary: %d, Slice length is %d", idx, len(rf.log)))
	}
	return (rf.log)[idx-1].Term
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

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state == LEADER
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state == CANDIDATE
}

// getElectionTimeout
// Generates election time out for leader election.
// return a random amount of time between 50 and 350
// milliseconds.
func getElectionTimeout() time.Duration {
	ms := 50 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}
