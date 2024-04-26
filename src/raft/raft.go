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

const INITIAL_TERM = 1

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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting RequestVoteArg
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update it self
	VoteGranted bool // true means candidate receive the vote
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log

	LeaderCommit int //  leader commit index.
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type AppendEntriesResult struct {
	server       int
	Success      bool
	LastEntry    bool
	PrevLogIndex int
	PrevLogTerm  int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.CandidateId == rf.me {
		return
	}

	// Not grant vote if voter's Term is greater than the candidate.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.lastLogIndex() > args.LastLogIndex || rf.lastLogIndex() == args.LastLogIndex && rf.lastLogTerm() > args.LastLogTerm {
		rf.debug(VOTING, "Not grant vote because my log is more update to date last idx (%d), last term (%d) and request is %+v", rf.lastLogIndex(), rf.lastLogTerm(), args)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.voteFor = args.CandidateId
		rf.electionTimeout = time.Now().Add(getElectionTimeout())

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	if rf.voteFor == args.CandidateId || rf.voteFor == -1 {
		reply.VoteGranted = true
		reply.Term = args.Term

		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.voteFor = args.CandidateId
		rf.electionTimeout = time.Now().Add(getElectionTimeout())
		return
	} else {
		rf.debug(VOTING, "Ask for Candidate %d,Already voted for %d", args.CandidateId, rf.voteFor)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//util.Println("%d received AppendEntries from %d for term %d, its term is %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//rf.debug(EVENT, "Start appending Entries")
	//defer rf.debug(EVENT, "End appending Entries")
	rf.debug(EVENT, "Request: %+v", args)
	rf.debug(EVENT, "%+v", args)

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex > rf.lastLogIndex() || args.PrevLogIndex < len(rf.log)+1 && args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		rf.debug(WARN, "I can't append the entry because the previous log is different.)")
		rf.debug(WARN, "Request: %+v", args)
		rf.debug(WARN, "%+v", rf)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.electionTimeout = time.Now().Add(getElectionTimeout())

	// Server finds new leader.
	if rf.state != FOLLOWER {
		rf.debug(VOTING, "See a new leader")
	}
	rf.state = FOLLOWER
	rf.currentTerm = args.Term
	rf.voteFor = -1

	rf.debug(INFO, "Receive entry %+v", args.Entries)
	for _, entry := range args.Entries {
		i := entry.Index
		if i == len(rf.log)+1 {
			rf.debug(WARN, "Append a new entry.")
			rf.debug(WARN, "%+v", args)
			rf.debug(WARN, "%+v", rf)
			rf.log = append(rf.log, Log{
				Command: entry.Command,
				Term:    entry.Term,
				Index:   entry.Index,
			})
		} else if i <= len(rf.log) {
			if rf.log[i-1].Term != entry.Term {
				rf.log[i-1] = Log{
					Command: entry.Command,
					Term:    entry.Term,
					Index:   entry.Index,
				}
				rf.log = rf.log[:i]
			}
		} else {
			rf.debug(WARN, "The Entry idx is illegal to append to the logs.")
			rf.debug(WARN, "%+v", args)
			rf.debug(WARN, "%+v", rf)
		}
		rf.debug(LOG_REPLICATING, "Appended a entry into log")
		rf.debugState()
	}
	if args.LeaderCommit > rf.commitIndex && len(rf.log) > 0 {
		lastEntryIndex := lastEntry(&rf.log).Index
		rf.commitIndex = min(lastEntryIndex, args.LeaderCommit)
		rf.debug(EVENT, "Commit to %d", args.LeaderCommit)
		rf.debugState()
		for i := rf.lastApplied; i < rf.commitIndex; i++ {
			entry := rf.log[i]
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

	reply.Term = rf.currentTerm
	reply.Success = true
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

	go rf.replicateLogsToPeers(cmd)

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
		rf.mu.Unlock()
		if state == FOLLOWER || state == CANDIDATE {
			if state == CANDIDATE {
				rf.debug(VOTING, "Last election failed, I am running a re-election!")
			}
			rf.mu.Lock()
			timeout := rf.electionTimeout
			rf.mu.Unlock()
			if time.Now().Before(timeout) {
				continue
			}

			if ok := rf.sendRequestVote(rf.me, &RequestVoteArgs{
				CandidateId: rf.me,
			}, &RequestVoteReply{}); !ok {
				rf.mu.Lock()
				electionTimeout := getElectionTimeout()
				rf.electionTimeout = time.Now().Add(electionTimeout)
				rf.mu.Unlock()
				time.Sleep(electionTimeout)
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
			rf.mu.Lock()
			electionTimeout := getElectionTimeout()
			rf.electionTimeout = time.Now().Add(electionTimeout)
			rf.mu.Unlock()
			time.Sleep(electionTimeout)
		}
	}
}

func (rf *Raft) electionCounting(votes *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	*votes++
	if *votes > len(rf.peers)/2 {
		if rf.state != LEADER {
			rf.debug(EVENT|VOTING, "Become a leader. term is %d", rf.currentTerm)
			rf.state = LEADER
			go rf.heartbeat()
		}
	}
}

func (rf *Raft) replicateLogsToPeers(cmd interface{}) {
	rf.mu.Lock()
	resultChan := make(chan AppendEntriesResult)
	quitChan := make(chan int)

	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	logIndex := len(rf.log) + 1
	currentTerm := rf.currentTerm
	newEntry := Log{cmd, currentTerm, logIndex}
	rf.log = append(rf.log, newEntry)

	successCnt := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateLog(i, lastLogIndex, lastLogTerm, newEntry.Index, newEntry, resultChan, quitChan)
	}
	rf.mu.Unlock()

	for {
		select {
		case result := <-resultChan:
			if result.Success {
				if result.LastEntry {
					rf.tryCommitEntry(&successCnt, newEntry)
					rf.updatePeerIndexes(result.server, newEntry.Index)
					rf.debugState()
				} else {
					rf.mu.Lock()
					prevLogIndex := result.PrevLogIndex + 1
					index := prevLogIndex + 1
					prevLogTerm := logTermAt(&rf.log, prevLogIndex)
					entry := rf.log[index-1]

					rf.debug(LOG_REPLICATING, "Resend replicate log request to %d, prevLogIndex %d", result.server, prevLogIndex)
					go rf.replicateLog(result.server, prevLogIndex, prevLogTerm, newEntry.Index, entry, resultChan, quitChan)
					rf.mu.Unlock()
				}
			} else {
				rf.resendAppendEntriesResult(result, resultChan, quitChan)
			}
		case <-quitChan:
			rf.debug(WARN, "Quit replicateLogsToPeer")
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) resendAppendEntriesResult(result AppendEntriesResult, resultChan chan AppendEntriesResult, quitChan chan int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogIndex := result.PrevLogIndex - 1
	logIndex := prevLogIndex + 1
	prevLogTerm := logTermAt(&rf.log, prevLogIndex)
	entry := rf.log[logIndex-1]

	rf.debug(LOG_REPLICATING, "Resend replicate log request, prevLogIndex %d", prevLogIndex)
	go rf.replicateLog(result.server, prevLogIndex, prevLogTerm, rf.lastLogIndex(), entry, resultChan, quitChan)
}

func (rf *Raft) replicateLog(i int, prevLogIndex int, prevLogTerm int, lastLogIndex int, entry Log, resultChan chan AppendEntriesResult, quitChan chan int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []Log{entry},
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, &args, reply)

	if !ok {
		return
	}

	if !rf.isLeader() || !reply.Success && rf.receiveHigherTerm(reply.Term) {
		quitChan <- 0
		return
	}

	if reply.Success {
		rf.debug(LOG_REPLICATING|EVENT, "Receive success replicate log response from %d", i)
		resultChan <- AppendEntriesResult{
			server:       i,
			Success:      true,
			LastEntry:    entry.Index == lastLogIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
		}
		return
	} else {
		rf.debug(LOG_REPLICATING|EVENT, "Receive fail replicate log response from %d", i)
		resultChan <- AppendEntriesResult{
			server:       i,
			Success:      false,
			LastEntry:    entry.Index == lastLogIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
		}
	}
}

func (rf *Raft) updatePeerIndexes(server, logIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex[server] = max(rf.nextIndex[server], logIndex+1)
	rf.matchIndex[server] = max(rf.matchIndex[server], logIndex)
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

func (rf *Raft) heartbeat() {
	resultChan := make(chan AppendEntriesResult)
	quitChan := make(chan int)
	go rf.handleFailureHeartBeat(resultChan, quitChan)

	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		currentTerm := rf.currentTerm
		commitIdx := rf.commitIndex
		rf.mu.Unlock()
		if state != LEADER {
			quitChan <- 0
			break
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int, resultChan chan AppendEntriesResult, quitChan chan int) {
				rf.mu.Lock()
				lastLogIndex := rf.lastLogIndex()
				lastLogTerm := rf.lastLogTerm()
				args := AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: lastLogIndex,
					PrevLogTerm:  lastLogTerm,
					Entries:      []Log{},
					LeaderCommit: commitIdx,
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					return
				}
				if !rf.isLeader() || ok && !reply.Success && rf.receiveHigherTerm(reply.Term) {
					quitChan <- 0
					return
				}

				if !reply.Success {
					rf.debug(WARN, "Fail heartbeat for server %d", i)
					resultChan <- AppendEntriesResult{
						server:       i,
						Success:      false,
						PrevLogIndex: lastLogIndex,
						PrevLogTerm:  lastLogTerm,
						LastEntry:    true,
					}
				} else {
					resultChan <- AppendEntriesResult{
						server:       i,
						Success:      true,
						PrevLogIndex: lastLogIndex,
						PrevLogTerm:  lastLogTerm,
						LastEntry:    true,
					}
				}
			}(i, resultChan, quitChan)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) handleFailureHeartBeat(resultChan chan AppendEntriesResult, quitChan chan int) {
	for {
		select {
		case result := <-resultChan:
			if result.Success {
				if result.LastEntry {
					rf.debugState()
				} else {
					rf.mu.Lock()
					prevLogIndex := result.PrevLogIndex + 1
					index := prevLogIndex + 1
					prevLogTerm := logTermAt(&rf.log, prevLogIndex)
					entry := rf.log[index-1]

					rf.debug(WARN, "Resend replicate log request, prevLogIndex %d", prevLogIndex)
					go rf.replicateLog(result.server, prevLogIndex, prevLogTerm, rf.lastLogIndex(), entry, resultChan, quitChan)
					rf.mu.Unlock()
				}
			} else {
				rf.resendAppendEntriesResult(result, resultChan, quitChan)
			}
		case <-quitChan:
			rf.debug(WARN, "Quit replicateLogsToPeer")
			return
		default:
			time.Sleep(10 * time.Millisecond)
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

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	rf.mu.Unlock()

	return rf.state == LEADER
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	rf.mu.Unlock()

	return rf.state == CANDIDATE
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

	rf.currentTerm = INITIAL_TERM
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
