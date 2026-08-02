package raft

import "time"

// Check if a leader election should be started.
// pause for a random amount of time between 50 and 350
// milliseconds.
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.lastReset) >= rf.electionTimeout {
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
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastReset = time.Now()
	rf.role = Candidate
	rf.resetElectionTimeout()

	rf.mu.Unlock()
	votes := 1
	for peer := range rf.peers {
		rf.mu.Lock()
		if peer == rf.me {
			rf.mu.Unlock()
			continue
		}
		lastLogIndex := len(rf.log) - 1
		rf.mu.Unlock()
		go func(p int) {
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogTerm:  rf.log[lastLogIndex].Term,
				LastLogIndex: lastLogIndex,
			}
			rf.mu.Unlock()
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(p, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.role != Candidate {
					DPrintf("Server %d is a %d\n", rf.me, rf.role)
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					votes++

					if votes > len(rf.peers)/2 {
						DPrintf("Server %d Won Election\n", rf.me)
						rf.role = Leader
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}
						go rf.sendToFollowers()

					}
				}
			}
		}(peer)
	}
}
