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
				if rf.nextIndex[p] <= rf.log.snapshotIndex() {
					// peer is behind the snapshot: send it instead of log entries.
					rf.mu.Unlock()
					rf.sendInstallSnapshotsToPeer(p)
					return
				}
				prevLogIndex := rf.nextIndex[p] - 1
				prevLogTerm := rf.log.entry(prevLogIndex).Term
				var entries []LogEntry
				if rf.log.len() > rf.nextIndex[p] {
					entries = rf.log.since(rf.nextIndex[p])
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
						if newMatch > rf.matchIndex[p] {
							rf.matchIndex[p] = newMatch
							rf.nextIndex[p] = newMatch + 1
							rf.updateCommitIndex()
						}
					} else {
						// back up nextIndex using the conflicting-entry
						// information from the follower. nextIndex may
						// drop below the snapshot index; the next tick
						// then switches to InstallSnapshot.
						rf.nextIndex[p] = backoffNextIndex(args.PrevLogIndex, &rf.log, reply)
					}
				}
			}(peer)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	for n := rf.log.lastIndex(); n > rf.commitIndex && n > rf.log.snapshotIndex(); n-- {
		if rf.log.entry(n).Term != rf.CurrentTerm {
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

func (rf *Raft) sendInstallSnapshotsToPeer(peer int) {
	rf.mu.Lock()
	if rf.Role != Leader {
		rf.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.log.snapshotIndex(),
		LastIncludedTerm:  rf.log.snapshotTerm(),
		Data:              rf.persister.ReadSnapshot(), // Read snapshot bytes from storage
	}
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peer, args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.Role != Leader || rf.CurrentTerm != args.Term {
		return
	}

	if reply.Term > rf.CurrentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	// Unconditionally move past the installed snapshot. A stale
	// matchIndex (advanced by an out-of-order AppendEntries reply)
	// must not keep nextIndex pinned at/below the snapshot, or the
	// leader would re-send the same snapshot forever.
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
}
