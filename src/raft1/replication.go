package raft

import "time"

func (rf *Raft) sendToFollowers() {
	for {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			go func(p int) {
				rf.mu.Lock()

				prevLogIndex := rf.nextIndex[peer] - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				var entries []LogEntry
				if len(rf.log) > rf.nextIndex[peer] {
					entries = append([]LogEntry{}, rf.log[rf.nextIndex[peer]:]...)
				}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()
				reply := &AppendEntriesReply{}

				if rf.sendAppendEntries(p, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.role != Leader || args.Term != rf.currentTerm {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
					}
					if reply.Success {
						rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[peer] = rf.matchIndex[peer] + 1

						rf.updateCommitIndex()
					} else {
						rf.nextIndex[peer] = max(1, rf.nextIndex[peer]-1)
					}
				}
			}(peer)
		}
		time.Sleep(100 * time.Millisecond)

	}
}

func (rf *Raft) updateCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		count := 1
		for peer, matchIdx := range rf.matchIndex {
			if peer != rf.me && matchIdx >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.commit <- true
			break
		}
	}
}
