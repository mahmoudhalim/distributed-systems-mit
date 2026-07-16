package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
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
	Message string
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []LogEntry
	role        Role
	// state if this server is leader
	nextIndex  []int
	matchIndex []int

	lastReset       time.Time
	electionTimeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		lastLogIndex := len(rf.log) - 1
		if args.LastLogTerm > rf.log[lastLogIndex].Term ||
			(args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.lastReset = time.Now()
		}
	}
	fmt.Printf("DEBUG: Server %d Got vote from Server %d ? %t\n", args.CandidateID, rf.me, reply.VoteGranted)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.role = Follower
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.lastReset = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.lastReset) >= rf.electionTimeout {
			fmt.Printf("DEBUG: Server %d Election Timed Out\n", rf.me)
			go rf.startElection()
		}
		timeout := rf.electionTimeout
		rf.mu.Unlock()
		time.Sleep(timeout)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	fmt.Printf("DEBUG: Server %d Started Election\n", rf.me)
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
					fmt.Printf("DEBUG: Server %d is a %d\n", rf.me, rf.role)
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					return
				}
				if reply.VoteGranted {
					votes++

					if votes > len(rf.peers)/2 {
						fmt.Printf("DEBUG: Server %d Won Election\n", rf.me)
						rf.role = Leader
						go rf.sendHeartBeats()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) sendHeartBeats() {
	for {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		term := rf.currentTerm
		rf.mu.Unlock()
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			go func(p int) {
				rf.mu.Lock()
				args := &AppendEntriesArgs{
					Term:     term,
					LeaderID: rf.me,
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}

				if rf.sendAppendEntries(p, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
					}
				}
			}(peer)
			time.Sleep(100 * time.Millisecond)
		}

	}
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

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.lastReset = time.Now()
	rf.resetElectionTimeout()
	rf.log = []LogEntry{{}}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	fmt.Printf("DEBUG: Server %d Created\n", me)
	return rf
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Duration(300+rand.Intn(250)) * time.Millisecond
}
