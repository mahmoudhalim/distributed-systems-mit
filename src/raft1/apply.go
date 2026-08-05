package raft

import "6.5840/raftapi"

func (rf *Raft) applyCommand() {
	for range rf.commit {
		rf.mu.Lock()
		var msgs []raftapi.ApplyMsg
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log.entry(i).Command,
				CommandIndex: i,
			})
			DPrintf("Server %d committed entry %d\n", rf.me, i)
			rf.lastApplied++
		}
		rf.mu.Unlock()
		for _, m := range msgs {
			rf.applyCh <- m
		}
	}
}
