package raft

import "time"

// becomeFollower steps this server down to a follower of the given
// term, clearing any vote it may have cast. The caller must hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = -1
	rf.lastReset = time.Now()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		lastLogIndex := len(rf.log) - 1
		if args.LastLogTerm > rf.log[lastLogIndex].Term ||
			(args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.lastReset = time.Now()
		}
	}
	DPrintf("Server %d Got vote from Server %d ? %t\n", args.CandidateID, rf.me, reply.VoteGranted)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d got entry %d from %d\n", rf.me, args.PrevLogTerm, args.LeaderID)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.role = Follower
	reply.Term = rf.currentTerm
	rf.lastReset = time.Now()

	if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}

	i := 0
	for ; i < len(args.Entries); i++ {
		idx := args.PrevLogIndex + 1 + i
		if idx >= len(rf.log) || rf.log[idx].Term != args.Entries[i].Term {
			break
		}
	}
	if i < len(args.Entries) {
		rf.log = rf.log[:args.PrevLogIndex+1+i]
		rf.log = append(rf.log, args.Entries[i:]...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.commit <- true
	}
	DPrintf("Server %d added entry %d to its log (length: %d)\n", rf.me, args.PrevLogTerm, len(rf.log))

	reply.Success = true
}
