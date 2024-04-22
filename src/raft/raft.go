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

// as each Raft peer becomes aware that successive log entries are
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
	isleader := rf.state == LEADER
	return term, isleader
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
	Term        int  // currentTerm, for can didate to update it self
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Not grant vote if voter's Term is greater than the candidate.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm || rf.voteFor == args.CandidateId || rf.voteFor == -1 {
		reply.VoteGranted = true
		reply.Term = args.Term

		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.voteFor = args.CandidateId
		rf.electionTimeout = time.Now().Add(getElectionTimeout())
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//util.Println("%d received AppendEntries from %d for term %d, its term is %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex > len(rf.log) || (args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		rf.debug(WARN, "I can't append the entry because the previous log is different")
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Server finds new leader.
	if rf.me != args.LeaderId {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
	}
	rf.voteFor = -1

	for _, entry := range args.Entries {
		i := entry.Index
		if i == len(rf.log)+1 {
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
		}
	}
	if args.LeaderCommit > rf.commitIndex && len(rf.log) > 0 {
		lastEntryIndex := lastEntry(&rf.log).Index
		rf.commitIndex = max(lastEntryIndex, args.LeaderCommit)
		for i := rf.lastApplied; i < rf.commitIndex; i++ {
			entry := rf.log[i]
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- msg
		}
	}

	rf.electionTimeout = time.Now().Add(getElectionTimeout())

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.debugState()
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
	rf.debug(RPC, "Send AppendEntries to CLIENT(%d)", server)
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
	rf.debug(INFO, "Receive command")
	rf.debugState()
	rf.mu.Lock()
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	rf.mu.Unlock()
	if !isLeader {
		return index, term, isLeader
	}

	go rf.replicateLogs(cmd)

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
			rf.mu.Lock()
			timeout := rf.electionTimeout
			rf.mu.Unlock()
			if time.Now().Before(timeout) {
				continue
			}

			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.currentTerm++
			rf.voteFor = rf.me
			term := rf.currentTerm
			rf.mu.Unlock()

			votes := 1
			for idx := range rf.peers {
				if idx == rf.me {
					continue
				}
				go func(i int) {
					args := RequestVoteArgs{
						Term:        term,
						CandidateId: rf.me,
					}
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(i, &args, &reply)

					rf.mu.Lock()
					state := rf.state
					currentTerm := rf.currentTerm
					rf.mu.Unlock()
					if state == FOLLOWER || state == LEADER {
						return
					}
					if ok && reply.VoteGranted {
						rf.mu.Lock()
						votes++
						v := votes
						rf.mu.Unlock()
						if v > len(rf.peers)/2 {
							rf.mu.Lock()
							rf.debug(EVENT, "Become a leader.")
							rf.state = LEADER
							rf.mu.Unlock()
							go rf.heartbeat()
						}
					} else if ok && reply.Term > currentTerm {
						rf.largerTerm(reply.Term)
					}
				}(idx)
			}
			rf.mu.Lock()
			electionTimeout := getElectionTimeout()
			rf.electionTimeout = time.Now().Add(electionTimeout)
			rf.mu.Unlock()
			time.Sleep(electionTimeout)
		}
	}
}

func (rf *Raft) replicateLogs(cmd interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logIndex := len(rf.log) + 1
	rf.log = append(rf.log, Log{cmd, rf.currentTerm, logIndex})
	rf.nextIndex[rf.me] += 1
	rf.matchIndex[rf.me] += 1
	currentTerm := rf.currentTerm
	successCnt := 1
	commitIdx := rf.commitIndex
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := logTermAt(&rf.log, prevLogIndex)
			rf.mu.Unlock()
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []Log{{cmd, currentTerm, logIndex}},
				LeaderCommit: commitIdx,
			}

			reply := &AppendEntriesReply{}

			ok := rf.sendAppendEntries(i, &args, reply)

			rf.mu.Lock()
			state := rf.state
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			if state != LEADER {
				return
			}
			if ok && !reply.Success && reply.Term > currentTerm {
				rf.largerTerm(reply.Term)
			}

			if reply.Success {
				rf.mu.Lock()
				rf.debug(EVENT, "Receive success replicate reponse, now total Cnt is %d", successCnt)
				successCnt += 1

				// Leader will commit the entry if find majority followers append the entries.
				if successCnt > len(rf.peers)/2 {
					rf.commitIndex = max(rf.commitIndex, logIndex)
					if logIndex == rf.lastApplied+1 {
						msg := ApplyMsg{
							CommandValid: true,
							Command:      cmd,
							CommandIndex: logIndex,
						}
						rf.applyCh <- msg
						rf.lastApplied = logIndex
					}
				}
				rf.debugState()
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		currentTerm := rf.currentTerm
		commitIdx := rf.commitIndex
		rf.mu.Unlock()
		if state != LEADER {
			break
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := logTermAt(&rf.log, prevLogIndex)
				rf.mu.Unlock()
				args := AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      []Log{},
					LeaderCommit: commitIdx,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)

				rf.mu.Lock()
				state := rf.state
				currentTerm = rf.currentTerm
				rf.mu.Unlock()
				if state != LEADER {
					return
				}
				if ok && !reply.Success && reply.Term > currentTerm {
					rf.largerTerm(reply.Term)
					return
				}
			}(i)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) largerTerm(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.electionTimeout = time.Now().Add(getElectionTimeout())
	rf.voteFor = -1
	rf.mu.Unlock()
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
