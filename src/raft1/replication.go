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
				rf.mu.Unlock()
				callPeer(rf, p, Leader,
					func() (*AppendEntriesArgs, *AppendEntriesReply) {
						prevLogIndex := rf.nextIndex[p] - 1
						prevLogTerm := rf.log.entry(prevLogIndex).Term
						var entries []LogEntry
						if rf.log.len() > rf.nextIndex[p] {
							entries = rf.log.since(rf.nextIndex[p])
						}
						return &AppendEntriesArgs{
							Term:         rf.CurrentTerm,
							LeaderID:     rf.me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogTerm,
							Entries:      entries,
							LeaderCommit: rf.commitIndex,
						}, &AppendEntriesReply{}
					},
					rf.sendAppendEntries,
					func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
						if reply.Term > rf.CurrentTerm {
							rf.becomeFollower(reply.Term)
							return
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
					})
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
	callPeer(rf, peer, Leader,
		func() (*InstallSnapshotArgs, *InstallSnapshotReply) {
			return &InstallSnapshotArgs{
				Term:              rf.CurrentTerm,
				LeaderID:          rf.me,
				LastIncludedIndex: rf.log.snapshotIndex(),
				LastIncludedTerm:  rf.log.snapshotTerm(),
				Data:              rf.persister.ReadSnapshot(), // Read snapshot bytes from storage
			}, &InstallSnapshotReply{}
		},
		rf.sendInstallSnapshot,
		func(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
			if reply.Term > rf.CurrentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			rf.nextIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex+1)
			rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
		})
}
