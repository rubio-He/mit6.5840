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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

// Raft State in election.
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

// A Go object implementing a single Raft peer.
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

	appendEntriesResultCh []chan AppendEntriesResult
	leaderQuitCh          chan int
	newEntriesCh          chan Log
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
//
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

type AppendEntriesResult struct {
	Success      bool
	LastEntry    bool
	PrevLogIndex int
	PrevLogTerm  int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.debug(RPC, "Send AppendEntries to CLIENT(%d)", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.debug(RPC, "Send Request Vote to CLIENT(%d)", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	rf.debug(WARN, "Receive Command %d", cmd)
	rf.mu.Lock()
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	rf.mu.Unlock()
	if !isLeader {
		return index, term, isLeader
	}

	// go rf.replicateLogsToPeers(cmd)

	return index, term, isLeader
}

func (rf *Raft) tryCommitEntry(successCnt *int, entry Log) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	*successCnt += 1
	rf.debug(EVENT|LOG_REPLICATING, "Receive success replicate response, now total Cnt is %d", *successCnt)

	// Leader will commit the entry if it finds the majority of followers have appended the entries.
	if *successCnt > len(rf.peers)/2 {
		rf.commitIndex = max(rf.commitIndex, entry.Index)
		if entry.Index == rf.lastApplied+1 {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- msg
			rf.lastApplied = entry.Index
			rf.debug(APPLY, "Applied Message of %+v", msg)
		}
	}
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
	ok := rf.sendRequestVote(rf.me, &RequestVoteArgs{
		CandidateId: rf.me,
	}, &RequestVoteReply{})
	return z == 1 && ok
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == FOLLOWER || state == CANDIDATE {
			rf.mu.Lock()
			timeout := rf.electionTimeout
			rf.mu.Unlock()
			if time.Now().Before(timeout) {
				continue
			}
			votes := 1

			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.currentTerm++
			rf.voteFor = rf.me
			currentTerm := rf.currentTerm
			lastLogIndex := rf.lastLogIndex()
			lastLogTerm := rf.lastLogTerm()
			rf.mu.Unlock()

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

					if !rf.isCandidate() || ok && rf.receiveHigherTerm(reply.Term) {
						return
					}

					if ok && reply.VoteGranted {
						rf.electionCounting(&votes)
					}
				}(idx, currentTerm)
			}
			electionTimeout := rf.resetElectionTimeout()
			time.Sleep(electionTimeout)
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
	rf.debug(EVENT|VOTING, "Become a leader. term is %d", rf.currentTerm)
	rf.state = LEADER
	// Initialize the next indexes to last log index + 1.
	// Initialize the match indexes to 0.
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	// Clean the leader quit channal.
	rf.leaderQuitCh = make(chan int)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		go rf.heartbeat(i, ticker)
	}
}

func (rf *Raft) heartbeat(i int, timer *time.Ticker) {
	for !rf.killed() {
		select {
		case <-timer.C:
			go rf.sendHeartbeat(i)
		case result := <-rf.appendEntriesResultCh[i]:
			if result.Success {
				if result.LastEntry {
					rf.debugState()
				} else {
					rf.handleAppendEntriesSuccess(i, result)
				}
			} else {
				rf.handleAppendEntriesFailure(i, result)
			}
			timer.Reset(10 * time.Second)
		case <-rf.leaderQuitCh:
			return
		}
	}
}

func (rf *Raft) handleAppendEntriesSuccess(i int, result AppendEntriesResult) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogIndex := result.PrevLogIndex + 1
	prevLogTerm := logTermAt(&rf.log, prevLogIndex)
	entry := rf.log[prevLogIndex]

	// Increase the followers' next index.
	rf.nextIndex[i] = prevLogIndex + 1
	// Increase the followers' match index.
	rf.matchIndex[i] = result.PrevLogIndex

	rf.debug(LOG_REPLICATING, "Resend replicate log request to %d, prevLogIndex %d", i, prevLogIndex)
	go rf.replicateLog(i, prevLogIndex, prevLogTerm, entry)
}

func (rf *Raft) handleAppendEntriesFailure(i int, result AppendEntriesResult) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogIndex := result.PrevLogIndex - 1
	prevLogTerm := logTermAt(&rf.log, prevLogIndex)
	entry := rf.log[prevLogIndex]

	// Decrease the followers' next index.
	rf.nextIndex[i] = prevLogIndex + 1

	rf.debug(LOG_REPLICATING, "Resend replicate log request, prevLogIndex %d", prevLogIndex)
	go rf.replicateLog(i, prevLogIndex, prevLogTerm, entry)
}

func (rf *Raft) replicateLog(i int, prevLogIndex int, prevLogTerm int, entry Log) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []Log{entry},
		LeaderCommit: rf.commitIndex,
	}
	leaderLastLogIndex := rf.lastLogIndex()
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, &args, reply)

	if !ok {
		return
	}

	if !rf.isLeader() || !reply.Success && rf.receiveHigherTerm(reply.Term) {
		rf.leaderQuitCh <- 0
		return
	}

	if reply.Success {
		rf.debug(LOG_REPLICATING|EVENT, "Receive success replicate log response from %d", i)
		rf.appendEntriesResultCh[i] <- AppendEntriesResult{
			Success:      true,
			LastEntry:    entry.Index == leaderLastLogIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
		}
		return
	} else {
		rf.debug(LOG_REPLICATING|EVENT, "Receive fail replicate log response from %d", i)
		rf.appendEntriesResultCh[i] <- AppendEntriesResult{
			Success:      false,
			LastEntry:    entry.Index == leaderLastLogIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
		}
	}
}

func (rf *Raft) sendHeartbeat(i int) {
	rf.mu.Lock()
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  lastLogTerm,
		Entries:      []Log{},
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, &args, &reply)
	if !ok {
		return
	}
	if !rf.isLeader() || ok && !reply.Success && rf.receiveHigherTerm(reply.Term) {
		rf.leaderQuitCh <- 0
		return
	}

	if !reply.Success {
		rf.debug(WARN, "Fail heartbeat for server %d", i)
		rf.appendEntriesResultCh[i] <- AppendEntriesResult{
			Success:      false,
			PrevLogIndex: lastLogIndex,
			PrevLogTerm:  lastLogTerm,
			LastEntry:    true,
		}
	} else {
		rf.appendEntriesResultCh[i] <- AppendEntriesResult{
			Success:      true,
			PrevLogIndex: lastLogIndex,
			PrevLogTerm:  lastLogTerm,
			LastEntry:    true,
		}
	}
}

func (rf *Raft) receiveHigherTerm(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm >= term {
		return false
	} else {
		rf.debug(EVENT, "Receive higher term %d, not a leader now.", term)
		rf.currentTerm = term
		rf.state = FOLLOWER
		rf.electionTimeout = time.Now().Add(getElectionTimeout())
		rf.voteFor = -1
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
	rf.leaderQuitCh = make(chan int)
	rf.newEntriesCh = make(chan Log)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// getElectionTimeout
// Generates election time out for leader election.
// return a random amount of time between 50 and 350
// milliseconds.
func getElectionTimeout() time.Duration {
	ms := 50 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}
