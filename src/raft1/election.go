package raft

import "time"

// Check if a leader election should be started.
// pause for a random amount of time between 50 and 350
// milliseconds.
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
		rf.mu.Lock()
		if peer == rf.me {
			rf.mu.Unlock()
			continue
		}
		lastLogIndex := len(rf.Log) - 1
		rf.mu.Unlock()
		go func(p int) {
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.me,
				LastLogTerm:  rf.Log[lastLogIndex].Term,
				LastLogIndex: lastLogIndex,
			}
			rf.mu.Unlock()
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(p, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.Role != Candidate || args.Term != rf.CurrentTerm {
					DPrintf("Server %d is a %d\n", rf.me, rf.Role)
					return
				}

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
							rf.nextIndex[i] = len(rf.Log)
							rf.matchIndex[i] = 0
						}
						go rf.sendToFollowers()

					}
				}
			}
		}(peer)
	}
}
