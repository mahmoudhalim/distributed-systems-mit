package raft

import "time"

// Check if a leader election should be started.
// randomized election timeout between 300 and 550 milliseconds.
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		if rf.Role != Leader && time.Since(rf.lastReset) >= rf.electionTimeout {
			DPrintf("Server %d Election Timed Out\n", rf.me)
			go rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("Server %d Started Election\n", rf.me)
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.lastReset = time.Now()
	rf.Role = Candidate
	rf.resetElectionTimeout()
	rf.persist()
	rf.mu.Unlock()
	votes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			callPeer(rf, p, Candidate,
				func() (*RequestVoteArgs, *RequestVoteReply) {
					return &RequestVoteArgs{
						Term:         rf.CurrentTerm,
						CandidateID:  rf.me,
						LastLogTerm:  rf.log.lastTerm(),
						LastLogIndex: rf.log.lastIndex(),
					}, &RequestVoteReply{}
				},
				rf.sendRequestVote,
				func(args *RequestVoteArgs, reply *RequestVoteReply) {
					if reply.Term > rf.CurrentTerm {
						rf.becomeFollower(reply.Term)
						return
					}
					if reply.VoteGranted {
						votes++

						if votes > len(rf.peers)/2 {
							DPrintf("Server %d Won Election\n", rf.me)
							rf.Role = Leader
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							for i := range rf.peers {
								rf.nextIndex[i] = rf.log.len()
								rf.matchIndex[i] = rf.log.snapshotIndex()
							}
							go rf.sendToFollowers()
						}
					}
				})
		}(peer)
	}
}
