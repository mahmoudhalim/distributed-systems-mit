package raft

import "time"

func (rf *Raft) sendToFollowers() {
	for {
		rf.mu.Lock()
		if rf.Role != Leader {
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
				prevLogTerm := rf.Log[prevLogIndex].Term
				var entries []LogEntry
				if len(rf.Log) > rf.nextIndex[peer] {
					entries = append([]LogEntry{}, rf.Log[rf.nextIndex[peer]:]...)
				}
				args := &AppendEntriesArgs{
					Term:         rf.CurrentTerm,
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

					if rf.Role != Leader || args.Term != rf.CurrentTerm {
						return
					}
					if reply.Term > rf.CurrentTerm {
						rf.becomeFollower(reply.Term)
					}
					if reply.Success {
						newMatch := args.PrevLogIndex + len(args.Entries)
						if newMatch > rf.matchIndex[peer] {
							rf.matchIndex[peer] = newMatch
							rf.nextIndex[peer] = newMatch + 1
							rf.updateCommitIndex()
						}
					} else {
						// back up nextIndex by more than one entry using the
						// conflicting-entry information from the follower.
						switch {
						case reply.XLen > 0 && args.PrevLogIndex >= reply.XLen:
							// follower's log is too short.
							rf.nextIndex[peer] = max(1, reply.XLen)
						case reply.XTerm > 0:
							// find the last index in our log with that term.
							idx := args.PrevLogIndex
							if idx >= len(rf.Log) {
								idx = len(rf.Log) - 1
							}
							for idx > 0 && rf.Log[idx].Term > reply.XTerm {
								idx--
							}
							if idx > 0 && rf.Log[idx].Term == reply.XTerm {
								rf.nextIndex[peer] = max(1, idx+1)
							} else {
								// we don't have that term.
								rf.nextIndex[peer] = max(1, reply.XIndex)
							}
						default:
							rf.nextIndex[peer] = max(1, args.PrevLogIndex)
						}
					}
				}
			}(peer)
		}
		time.Sleep(100 * time.Millisecond)

	}
}

func (rf *Raft) updateCommitIndex() {
	for n := len(rf.Log) - 1; n > rf.commitIndex; n-- {
		if rf.Log[n].Term != rf.CurrentTerm {
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
