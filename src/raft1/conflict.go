package raft

// conflict.go holds the two conflict-resolution decisions of Raft as
// pure functions: the follower's checkAppend (is the leader's request
// applicable to my log, and how much already matches?) and the
// leader's backoffNextIndex (where do I resume replicating to a peer
// that just rejected my AppendEntries?).

// appendConflict describes why an AppendEntries request cannot be
// applied as-is: the entry at prevIndex is beyond the log (XLen set,
// the log's length) or its term differs (XTerm/XIndex set, the term
// and the first index of that term in this log).
type appendConflict struct {
	XLen   int
	XTerm  int
	XIndex int
}

// checkAppend inspects the follower's log l against an AppendEntries
// request and returns how many of the leader's entries already match
// (starting at prevIndex+1), or an appendConflict if the request
// cannot be applied.
func checkAppend(l *raftLog, prevIndex, prevTerm int, entries []LogEntry) (int, appendConflict) {
	if prevIndex < l.snapshotIndex() || prevIndex >= l.len() {
		// the entry at prevIndex is no longer kept (discarded by a
		// snapshot, or not yet present): report the log length so the
		// leader can back off.
		return 0, appendConflict{XLen: l.len()}
	}
	if l.entry(prevIndex).Term != prevTerm {
		c := appendConflict{XTerm: l.entry(prevIndex).Term, XIndex: prevIndex}
		for c.XIndex > l.snapshotIndex()+1 && l.entry(c.XIndex-1).Term == c.XTerm {
			c.XIndex--
		}
		return 0, c
	}
	return l.matchPrefix(prevIndex, entries), appendConflict{}
}

// backoffNextIndex computes the leader's nextIndex for a peer after a
// failed AppendEntries. If the follower's log was too short, jump
// past its length; if it reported a conflicting term, resume at the
// last entry of our log with that term (or at the follower's first
// index of that term when we no longer have it); otherwise back off
// one entry.
func backoffNextIndex(prevLogIndex int, l *raftLog, reply *AppendEntriesReply) int {
	switch {
	case reply.XLen > 0 && prevLogIndex >= reply.XLen:
		return max(1, reply.XLen)
	case reply.XTerm > 0:
		idx := prevLogIndex
		if idx >= l.len() {
			idx = l.lastIndex()
		}
		for idx > l.snapshotIndex() && l.entry(idx).Term > reply.XTerm {
			idx--
		}
		if idx > l.snapshotIndex() && l.entry(idx).Term == reply.XTerm {
			return max(1, idx+1)
		}
		return max(1, reply.XIndex)
	default:
		return max(1, prevLogIndex)
	}
}
