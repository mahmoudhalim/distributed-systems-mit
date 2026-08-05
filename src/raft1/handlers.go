package raft

import (
	"time"

	"6.5840/raftapi"
)

// becomeFollower steps this server down to a follower of the given
// term, clearing any vote it may have cast. The caller must hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.CurrentTerm = term
	rf.Role = Follower
	rf.VotedFor = -1
	rf.persist()
}

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
		if args.LastLogTerm > rf.log.lastTerm() ||
			(args.LastLogTerm == rf.log.lastTerm() && args.LastLogIndex >= rf.log.lastIndex()) {
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

	if args.PrevLogIndex >= rf.log.len() {
		// follower's log is too short: report its length so the
		// leader can back up nextIndex past it.
		reply.Success = false
		reply.XLen = rf.log.len()
		return
	}
	if args.PrevLogIndex >= rf.log.snapshotIndex() && rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		// conflicting entry: report the term and the first index of
		// that term in this server's log.
		reply.Success = false
		reply.XTerm = rf.log.entry(args.PrevLogIndex).Term
		reply.XIndex = args.PrevLogIndex
		for reply.XIndex > rf.log.snapshotIndex()+1 && rf.log.entry(reply.XIndex-1).Term == reply.XTerm {
			reply.XIndex--
		}
		return
	}

	i := rf.log.matchPrefix(args.PrevLogIndex, args.Entries)
	logChanged := false
	if i < len(args.Entries) {
		rf.log.truncateTo(args.PrevLogIndex + 1 + i)
		rf.log.append(args.Entries[i:]...)
		logChanged = true
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = max(rf.log.snapshotIndex(), min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
		rf.commit <- true
	}
	if logChanged {
		rf.persist()
	}
	DPrintf("Server %d added entry %d to its log (length: %d)\n", rf.me, args.PrevLogTerm, rf.log.len())

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.Role = Follower
	reply.Term = rf.CurrentTerm
	rf.lastReset = time.Now()

	if args.LastIncludedIndex <= rf.log.snapshotIndex() { // i have more recent Snapshot
		return
	}

	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.persister.Save(rf.encodeRaftState(), args.Data)
	rf.commit <- true

	msg := raftapi.ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  -1,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	go func() {
		rf.applyCh <- msg
	}()
}
