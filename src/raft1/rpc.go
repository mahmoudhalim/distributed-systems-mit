package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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

	// XTerm/XIndex/XLen allow the leader to back up nextIndex
	// by more than one entry at a time on log conflict.
	XTerm  int // term of the conflicting entry (0 if none)
	XIndex int // first index of the conflicting term in this server's log
	XLen   int // length of this server's log
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

// callPeer owns the lifecycle of an outbound peer RPC, identical for
// every RPC type: check the role and build the args under rf.mu, send
// the request without the lock, then reacquire the lock and run apply
// only if this server is still in role want with an unchanged term.
// A reply that arrives after stepping down must never be applied:
// that is how stale votes get counted and how a stale matchIndex
// advance wedges a leader. The guard lives here, once.
func callPeer[Args, Reply any](
	rf *Raft,
	peer int,
	want Role,
	build func() (Args, Reply),
	send func(peer int, args Args, reply Reply) bool,
	apply func(args Args, reply Reply),
) {
	rf.mu.Lock()
	if rf.Role != want {
		rf.mu.Unlock()
		return
	}
	args, reply := build()
	term := rf.CurrentTerm
	rf.mu.Unlock()

	if !send(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Role != want || rf.CurrentTerm != term {
		return
	}
	apply(args, reply)
}
