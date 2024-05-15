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
		rf.debug(VOTING, "See a higher term from client %d, term is %d, my term is %d. Vote Grant!", args.CandidateId, args.Term, rf.currentTerm)
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
