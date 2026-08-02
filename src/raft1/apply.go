package raft

import "6.5840/raftapi"

func (rf *Raft) applyCommand() {
	for range rf.commit {
		rf.mu.Lock()
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {

			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			DPrintf("Server %d committed entry %d\n", rf.me, i)

			rf.applyCh <- applyMsg
			rf.lastApplied++
		}
		rf.mu.Unlock()
	}
}
