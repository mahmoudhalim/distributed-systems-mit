package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	applyCh   chan raftapi.ApplyMsg
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	commit    chan bool

	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	Role        Role

	commitIndex int
	lastApplied int

	// state if this server is leader
	nextIndex  []int
	matchIndex []int

	lastReset       time.Time
	electionTimeout time.Duration

	// Snapshot state
	lastSnapshotIndex int
	lastSnapshotTerm  int
}

// return currentterm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.Role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	return w.Bytes()
}

func (rf *Raft) persist() {
	raftstate := rf.encodeRaftState()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, snapshot)
}

	// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("can't read persisted state")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
	}
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	if err := d.Decode(&lastSnapshotIndex); err != nil {
		// state persisted before snapshots were added
		lastSnapshotIndex = 0
		lastSnapshotTerm = 0
	} else {
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
	}
	if rf.lastSnapshotIndex > 0 {
		rf.commitIndex = rf.lastSnapshotIndex
		rf.lastApplied = rf.lastSnapshotIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastSnapshotIndex { // i have more updated snapshot
		return
	}
	rf.lastSnapshotTerm = rf.getEntry(index).Term
	rf.truncateLog(index)
	rf.lastSnapshotIndex = index
	raftState := rf.encodeRaftState()
	rf.persister.Save(raftState, snapshot)
}

func (rf *Raft) truncateLog(index int) {
	idx := index - rf.lastSnapshotIndex
	dummyEntry := LogEntry{Term: rf.Log[idx].Term, Command: nil}

	if len(rf.Log) > idx+1 {
		rf.Log = append([]LogEntry{dummyEntry}, rf.Log[idx+1:]...)
	} else {
		rf.Log = []LogEntry{dummyEntry}
	}
}

func (rf *Raft) getEntry(index int) LogEntry {
	return rf.Log[index-rf.lastSnapshotIndex]
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.Role != Leader {
		return -1, -1, false
	}
	DPrintf("Server %d Got Command\n", rf.me)

	rf.Log = append(rf.Log, LogEntry{rf.CurrentTerm, command})
	rf.persist()
	return len(rf.Log) - 1 + rf.lastSnapshotIndex, rf.CurrentTerm, true
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Buffered: commit signals are sent while holding rf.mu (see
	// AppendEntries/updateCommitIndex/InstallSnapshot). If the apply
	// loop is blocked on applyCh (the applier does a synchronous RPC
	// to the tester), an unbuffered send would block forever while
	// holding the lock, wedging the whole server. Signals are just
	// wakeups; applyCommand re-reads commitIndex on every wakeup, so
	// stale/buffered signals are harmless.
	rf.commit = make(chan bool, 100)
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Role = Follower
	rf.lastReset = time.Now()
	rf.resetElectionTimeout()
	rf.Log = []LogEntry{{}}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommand()
	DPrintf("Server %d Created\n", me)
	return rf
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Duration(300+rand.Intn(250)) * time.Millisecond
}
