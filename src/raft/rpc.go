package raft

import "time"

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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm

	if args.CandidateId == rf.me {
		return
	}

	// Not grant vote if voter's Term is greater than the candidate.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
	}

	rf.debug(VOTING, "Candidate up-to-date check:,%d ,%d, %+v", rf.lastLogIndex(), rf.lastLogTerm(), args)
	if rf.lastLogTerm() > args.LastLogTerm || rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex {
		rf.debug(VOTING, "Not grant vote because my log is more update to date last idx (%d), last term (%d) and request is %+v", rf.lastLogIndex(), rf.lastLogTerm(), args)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > term {
		rf.debug(VOTING, "See a higher term from client %d, term is %d, my term is %d. Vote Grant!", args.CandidateId, args.Term, rf.currentTerm)
		rf.voteFor = args.CandidateId
		rf.electionTimeout = time.Now().Add(getElectionTimeout())

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}

	if rf.voteFor == args.CandidateId || rf.voteFor == -1 {
		rf.debug(VOTING, "vote grant")
		reply.VoteGranted = true
		reply.Term = args.Term

		rf.state = FOLLOWER
		rf.voteFor = args.CandidateId
		rf.electionTimeout = time.Now().Add(getElectionTimeout())
		rf.persist()
		return
	} else {
		rf.debug(VOTING, "Ask for Candidate %d,Already voted for %d", args.CandidateId, rf.voteFor)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//util.Println("%d received AppendEntries from %d for term %d, its term is %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.LeaderId == rf.me {
		return
	}

	if rf.currentTerm > args.Term {
		rf.debug(WARN, "Higher Term", args)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex > rf.lastLogIndex() || args.PrevLogIndex < len(rf.log)+1 && args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		rf.debug(WARN, "I can't append the entry because the previous log is different.)")
		rf.debug(WARN, "Request: %+v", args)
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
			rf.debug(WARN, "The LastEntryInRequest idx is illegal to append to the logs.")
			rf.debug(WARN, "%+v", args)
			rf.debug(WARN, "%+v", rf)
		}
		rf.debug(LOG_REPLICATING, "Appended a entry into log")
	}
	rf.persist()
	if args.LeaderCommit > rf.commitIndex && len(rf.log) > 0 {
		lastEntryIndex := lastEntry(&rf.log).Index
		rf.commitIndex = min(lastEntryIndex, args.LeaderCommit)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}
