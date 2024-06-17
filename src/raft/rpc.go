package raft

import (
	"sort"
	"time"
)

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

	// Log backtracking optimization
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.debug(RPC, "time: %s AppendEntries CLIENT(%d)", time.Now().String(), server)
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
	rf.debug(RPC, "time: %s Send Request Vote to CLIENT(%d)", time.Now().String(), server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.debug(RPC, "time: %s Send Install Snapshot to CLIENT(%d)", time.Now().String(), server)
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.debug(VOTING, "time: %s RequestVote(%d) to %d", time.Now().String(), args.Term, args.CandidateId)

	// Not grant vote if voter's Term is greater than the candidate.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.voteFor = args.CandidateId
	}

	if rf.lastLogTerm() > args.LastLogTerm || rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex {
		rf.debug(STATE, "Not grant vote because my log is more update to date last idx (%d), last term (%d) and request is %+v", rf.lastLogIndex(), rf.lastLogTerm(), args)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.voteFor != args.CandidateId && rf.voteFor != -1 {
		rf.debug(STATE, "Ask for Candidate %d,Already voted for %d", args.CandidateId, rf.voteFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.debug(VOTING, "vote grant")
	reply.VoteGranted = true
	reply.Term = args.Term

	rf.state = FOLLOWER
	rf.voteFor = args.CandidateId
	rf.electionTimeout = time.Now().Add(getElectionTimeout())
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.debugState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.voteFor = args.LeaderId
	}

	// Raft server doesn't contain the previous log.
	if args.PrevLogIndex > rf.lastLogIndex() {
		rf.debugState()
		rf.debug(STATE, "doesn't contain the previous log. %+v", args)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = max(rf.lastIncludeIndex+1, rf.lastLogIndex())
		return
	}

	// Raft contains the previous log, but with different term.
	if args.PrevLogIndex <= rf.lastLogIndex() && args.PrevLogIndex > 0 && rf.logTermAt(args.PrevLogIndex) != args.PrevLogTerm {
		rf.debugState()
		rf.debug(STATE, "with different term, %+v", args)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictTerm = rf.logTermAt(args.PrevLogIndex)
		pos, _ := sort.Find(len(rf.log), func(i int) int {
			return reply.ConflictTerm - rf.log[i].Term
		})
		reply.ConflictIndex = rf.log[pos].Index
		return
	}

	rf.state = FOLLOWER
	rf.currentTerm = args.Term
	rf.voteFor = args.LeaderId

	rf.debug(INFO, "Receive entry %+v", args.Entries)
	for _, entry := range args.Entries {
		i := entry.Index - rf.lastIncludeIndex
		if i == len(rf.log)+1 {
			rf.debug(STATE, "Appended a entry into log %+v", entry)
			rf.debugState()
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
			rf.debug(WARN, "WRONG")
		}
	}

	rf.electionTimeout = time.Now().Add(getElectionTimeout())

	if args.LeaderCommit > rf.commitIndex {
		lastEntryIndex := args.LeaderCommit
		if len(args.Entries) != 0 {
			lastEntryIndex = min(lastEntry(&args.Entries).Index, lastEntryIndex)
		}
		rf.commitIndex = lastEntryIndex
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else {
		rf.currentTerm = args.Term
		rf.voteFor = args.LeaderId
		rf.state = FOLLOWER
	}

	if rf.lastIncludeIndex >= args.LastIncludedIndex {
		return
	}

	// Follower transition and timeout reset.
	rf.state = FOLLOWER
	rf.currentTerm = args.Term
	rf.voteFor = -1
	rf.electionTimeout = time.Now().Add(getElectionTimeout())

	rf.snapshot = args.Data
	pos, found := sort.Find(len(rf.log), func(i int) int {
		return args.LastIncludedIndex - rf.log[i].Index
	})
	if found {
		rf.log = rf.log[pos+1:]
	} else {
		rf.log = []Log{}
	}
	rf.lastIncludeTerm = args.LastIncludedTerm
	rf.lastIncludeIndex = args.LastIncludedIndex
	rf.lastApplied = rf.lastIncludeIndex
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludeIndex)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
}
