package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// State in election.
type State int

const InitialTerm = 1

const (
	CANDIDATE State = iota
	LEADER
	FOLLOWER
)

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// state
	currentTerm int
	voteFor     int
	log         []Log // log entries, each entry contains command for state machine, and term when entry was received by leader, first index is 1.

	commitIndex int // Index of highest log entry known t obe committed
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state           State
	electionTimeout time.Time

	applyCh chan ApplyMsg

	lastIncludeIndex int
	lastIncludeTerm  int

	appendEntriesResultCh []chan AppendEntriesResult
	leaderQuitCh          chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.state == LEADER
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.voteFor)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	state := w.Bytes()
	rf.persister.Save(state, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var log []Log
	var voteFor int
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		rf.debug(PERSIST, "Decode Error")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.log = log
		rf.voteFor = voteFor
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
		rf.debug(PERSIST, "Restore success")
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index < rf.lastIncludeIndex {
		return
	}

}

type AppendEntriesResult struct {
	Success            bool
	LastEntryInRequest Log
	PrevLogIndex       int
	ConflictIndex      int
	ConflictTerm       int
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(cmd interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if !isLeader {
		return index, term, isLeader
	}
	//rf.debug(STATE, "A leader receive for cmd %d", cmd)
	newEntry := Log{cmd, rf.currentTerm, len(rf.log) + 1}
	rf.log = append(rf.log, newEntry)
	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		timeout := rf.electionTimeout
		rf.mu.Unlock()
		if state != LEADER && time.Now().After(timeout) {
			rf.mu.Lock()
			rf.state = CANDIDATE
			votes := 1
			rf.currentTerm++
			rf.voteFor = rf.me
			currentTerm := rf.currentTerm
			lastLogIndex := rf.lastLogIndex()
			lastLogTerm := rf.lastLogTerm()
			rf.persist()
			rf.mu.Unlock()
			rf.debug(STATE, "Start voting my term is %d", rf.currentTerm)
			for idx := range rf.peers {
				if idx == rf.me {
					continue
				}
				go func(i, currentTerm int) {
					args := RequestVoteArgs{
						Term:         currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					}
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(i, &args, &reply)

					if !rf.isCandidate() || ok && rf.receiveHigherTerm(i, reply.Term) {
						return
					}

					if ok && reply.VoteGranted {
						rf.electionCounting(&votes)
					}
				}(idx, currentTerm)
			}
		}
		electionTimeout := rf.resetElectionTimeout()
		time.Sleep(electionTimeout)
	}
}

func (rf *Raft) applier() {
	ticker := time.NewTicker(10 * time.Millisecond)
	for !rf.killed() {
		select {
		case <-ticker.C:
			rf.mu.Lock()
			for rf.commitIndex > rf.lastApplied && rf.lastApplied < len(rf.log) {
				toBeAppliedEntry := rf.log[rf.lastApplied]
				msg := ApplyMsg{
					CommandValid: true,
					Command:      toBeAppliedEntry.Command,
					CommandIndex: toBeAppliedEntry.Index,
				}
				rf.applyCh <- msg
				rf.lastApplied = toBeAppliedEntry.Index
				rf.debug(APPLY, "Applied Message of %+v", msg)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) resetElectionTimeout() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	electionTimeout := getElectionTimeout()
	rf.electionTimeout = time.Now().Add(electionTimeout)
	return electionTimeout
}

func (rf *Raft) electionCounting(votes *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	*votes++
	if *votes > len(rf.peers)/2 && rf.state != LEADER {
		rf.convertToLeader()
	}
}

func (rf *Raft) convertToLeader() {
	rf.debug(STATE, "become leader, cur term %d", rf.currentTerm)
	rf.state = LEADER
	// Initialize the next indexes to last log index + 1.
	// Initialize the match indexes to 0.
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		ticker := time.NewTicker(35 * time.Millisecond)
		go rf.heartbeat(i, ticker)
	}
}

func (rf *Raft) heartbeat(i int, ticker *time.Ticker) {
	for !rf.killed() {
		if !rf.isLeader() {
			break
		}
		select {
		case result := <-rf.appendEntriesResultCh[i]:
			rf.handleAppendEntries(i, result)
			if result.Success {
				rf.tryCommitEntry()
			}
		case <-ticker.C:
			go rf.sendHeartbeat(i)
		}
	}
}

func (result *AppendEntriesResult) isEmptyLog() bool {
	emptyLog := Log{}
	return result.LastEntryInRequest == emptyLog
}

func (rf *Raft) tryCommitEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	matchedIndex := make([]int, len(rf.matchIndex))
	copy(matchedIndex, rf.matchIndex)
	slices.Sort(matchedIndex)
	majorityCnt := len(matchedIndex)/2 + 1
	majorityMatchedIndex := matchedIndex[majorityCnt]
	// Leader will commit the entry if it finds the majority of followers have appended the entries.
	// Only commit to this log if this log is from its current Term.
	if rf.logTermAt(majorityMatchedIndex) == rf.currentTerm {
		rf.commitIndex = max(rf.commitIndex, majorityMatchedIndex)
	}

}

func (rf *Raft) handleAppendEntries(i int, result AppendEntriesResult) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if result.Success {
		if !result.isEmptyLog() {
			// Note: Because the log replication is async, leader will receive some already synced ack response.
			// Skip these responses.
			if result.LastEntryInRequest.Index+1 < rf.nextIndex[i] {
				return
			}
			rf.nextIndex[i] = result.LastEntryInRequest.Index + 1
		}
	} else {
		pos, found := sort.Find(len(rf.log), func(i int) int {
			return result.ConflictTerm - rf.log[i].Term
		})
		if found {
			rf.nextIndex[i] = rf.log[pos].Index
		} else {
			rf.nextIndex[i] = max(1, result.ConflictIndex)
		}
	}

	var entries []Log
	if result.Success {
		if rf.nextIndex[i]-1 < len(rf.log) {
			entries = rf.log[rf.nextIndex[i]-1 : min(rf.nextIndex[i]+40, len(rf.log))]
		}
	} else {
		entries = rf.log[rf.nextIndex[i]-1 : rf.nextIndex[i]]
	}

	rf.debugState()
	rf.debug(STATE, "%d, %+v", i, result)

	// update match index if success.
	if result.Success {
		rf.matchIndex[i] = max(rf.matchIndex[i], result.PrevLogIndex)
	}
	prevLogIndex := rf.nextIndex[i] - 1

	if len(entries) != 0 {
		go rf.replicateLog(i, prevLogIndex, rf.logTermAt(prevLogIndex), entries)
	}
}

func (rf *Raft) replicateLog(i int, prevLogIndex int, prevLogTerm int, entries []Log) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, &args, reply)

	if !ok || !rf.isLeader() {
		return
	}
	if rf.receiveHigherTerm(-1, reply.Term) {
		return
	}
	if !rf.isLeader() {
		return
	}

	result := AppendEntriesResult{
		Success:      reply.Success,
		PrevLogIndex: prevLogIndex,
	}

	if len(entries) > 0 {
		result.LastEntryInRequest = entries[len(entries)-1]
	}
	if !reply.Success {
		result.ConflictTerm = reply.ConflictTerm
		result.ConflictIndex = reply.ConflictIndex
	}
	rf.appendEntriesResultCh[i] <- result
}

func (rf *Raft) sendHeartbeat(i int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	peerNextIdx := rf.nextIndex[i]
	prevIdx := peerNextIdx - 1
	prevTerm := rf.logTermAt(prevIdx)

	var entries []Log
	if len(rf.log) >= peerNextIdx {
		entries = rf.log[peerNextIdx-1 : peerNextIdx]
	}
	rf.mu.Unlock()
	rf.replicateLog(i, prevIdx, prevTerm, entries)
}

func (rf *Raft) receiveHigherTerm(i int, term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm >= term {
		return false
	} else {
		rf.debug(STATE, "Receive higher term %d, not a leader now.", term)
		rf.currentTerm = term
		rf.state = FOLLOWER
		rf.electionTimeout = time.Now().Add(getElectionTimeout())
		rf.debug(WARN, "current time  is %s", time.Now())
		rf.voteFor = i
		rf.persist()
		return true
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = make([]Log, 0)

	rf.currentTerm = InitialTerm
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.state = FOLLOWER
	rf.electionTimeout = time.Now().Add(getElectionTimeout())

	rf.applyCh = applyCh
	rf.appendEntriesResultCh = make([]chan AppendEntriesResult, len(peers))
	for i := range peers {
		rf.appendEntriesResultCh[i] = make(chan AppendEntriesResult)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
