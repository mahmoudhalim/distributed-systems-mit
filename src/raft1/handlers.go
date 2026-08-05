package raft

import "time"

// becomeFollower steps this server down to a follower of the given
// term, clearing any vote it may have cast. The caller must hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.CurrentTerm = term
	rf.Role = Follower
	rf.VotedFor = -1
	rf.persist()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		lastLogIndex := len(rf.Log) - 1
		if args.LastLogTerm > rf.Log[lastLogIndex].Term ||
			(args.LastLogTerm == rf.Log[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateID
			rf.lastReset = time.Now()
		}
	}
	rf.persist()
	DPrintf("Server %d Got vote from Server %d ? %t\n", args.CandidateID, rf.me, reply.VoteGranted)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d got entry %d from %d\n", rf.me, args.PrevLogTerm, args.LeaderID)

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.Role = Follower
	reply.Term = rf.CurrentTerm
	rf.lastReset = time.Now()

	if args.PrevLogIndex >= len(rf.Log) {
		// follower's log is too short: report its length so the
		// leader can back up nextIndex past it.
		reply.Success = false
		reply.XLen = len(rf.Log)
		return
	}
	if args.PrevLogIndex > 0 && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// conflicting entry: report the term and the first index of
		// that term in this server's log.
		reply.Success = false
		reply.XTerm = rf.Log[args.PrevLogIndex].Term
		reply.XIndex = args.PrevLogIndex
		for reply.XIndex > 1 && rf.Log[reply.XIndex-1].Term == reply.XTerm {
			reply.XIndex--
		}
		return
	}

	i := 0
	for ; i < len(args.Entries); i++ {
		idx := args.PrevLogIndex + 1 + i
		if idx >= len(rf.Log) || rf.Log[idx].Term != args.Entries[i].Term {
			break
		}
	}
	logChanged := false
	if i < len(args.Entries) {
		rf.Log = rf.Log[:args.PrevLogIndex+1+i]
		rf.Log = append(rf.Log, args.Entries[i:]...)
		logChanged = true
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.commit <- true
	}
	if logChanged {
		rf.persist()
	}
	DPrintf("Server %d added entry %d to its log (length: %d)\n", rf.me, args.PrevLogTerm, len(rf.Log))

	reply.Success = true
}
