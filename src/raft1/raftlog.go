package raft

// raftLog is the Raft log: a sequence of entries spanning the absolute
// index range [snapIndex, snapIndex+len(entries)-1]. The entry at
// snapIndex is a synthetic dummy (Command nil) that exists so that
// index math stays uniform after truncation; by construction its Term
// always equals snapTerm.
//
// All methods speak absolute indices (the language of Raft's Figure 2);
// the conversion to slice offsets is private to this type. The only
// exception is persistEntries(), which exposes the raw slice for
// persistence encoding and must not be mutated by the caller.
//
// Not thread-safe: callers must hold rf.mu.
type raftLog struct {
	entries   []LogEntry
	snapIndex int
	snapTerm  int
}

// newRaftLog returns an empty log: a single dummy entry at index 0.
func newRaftLog() *raftLog {
	return &raftLog{
		entries: []LogEntry{{}},
	}
}

// restoreLog reconstructs a log from persisted state.
func restoreLog(entries []LogEntry, snapIndex, snapTerm int) *raftLog {
	return &raftLog{
		entries:   entries,
		snapIndex: snapIndex,
		snapTerm:  snapTerm,
	}
}

// len returns the length of the log in absolute indices:
// the index one past the last entry.
func (l *raftLog) len() int {
	return l.snapIndex + len(l.entries)
}

// lastIndex returns the absolute index of the last entry.
func (l *raftLog) lastIndex() int {
	return l.len() - 1
}

// lastTerm returns the term of the last entry.
func (l *raftLog) lastTerm() int {
	return l.entry(l.lastIndex()).Term
}

// entry returns the entry at absolute index i.
// The caller must ensure snapIndex <= i <= lastIndex(); violating the
// invariant panics, just like out-of-range slice access.
func (l *raftLog) entry(i int) LogEntry {
	return l.entries[i-l.snapIndex]
}

func (l *raftLog) snapshotIndex() int {
	return l.snapIndex
}

func (l *raftLog) snapshotTerm() int {
	return l.snapTerm
}

// append adds entries at the end of the log.
func (l *raftLog) append(entries ...LogEntry) {
	l.entries = append(l.entries, entries...)
}

// truncateTo drops every entry with absolute index > i, leaving the
// dummy at i (i must be >= snapIndex). Used after a log conflict:
// the caller then appends the new entries.
func (l *raftLog) truncateTo(i int) {
	cut := i - l.snapIndex
	if cut < 1 {
		cut = 1 // always keep the dummy
	}
	l.entries = l.entries[:cut]
}

// snapshotAt collapses the entry at absolute index i into the dummy:
// the entry at i becomes the new dummy (keeping its Term) and the tail
// after it is retained. Used when the service snapshots up to i, or
// when an InstallSnapshot covers an entry we still hold.
func (l *raftLog) snapshotAt(i int) {
	idx := i - l.snapIndex
	dummy := LogEntry{Term: l.entries[idx].Term}
	if len(l.entries) > idx+1 {
		l.entries = append([]LogEntry{dummy}, l.entries[idx+1:]...)
	} else {
		l.entries = []LogEntry{dummy}
	}
	l.snapIndex = i
	l.snapTerm = dummy.Term
}

// installSnapshot installs a snapshot covering up to absolute index
// index. If we still hold that entry and its term matches, the tail
// after it is kept; otherwise the log is replaced by a fresh dummy.
// snapIndex/snapTerm are updated to the installed snapshot.
func (l *raftLog) installSnapshot(index, term int) {
	if index < l.len() && l.entry(index).Term == term {
		l.snapshotAt(index)
	} else {
		l.entries = []LogEntry{{Term: term}}
		l.snapIndex = index
		l.snapTerm = term
	}
}

// matchPrefix returns how many consecutive entries of theirs match
// this log's entries starting at index prevIndex+1. The caller uses
// the result to find the first diverging entry (prevIndex+1+count)
// before truncating and appending.
func (l *raftLog) matchPrefix(prevIndex int, theirs []LogEntry) int {
	i := 0
	for ; i < len(theirs); i++ {
		idx := prevIndex + 1 + i
		if idx >= l.len() || l.entry(idx).Term != theirs[i].Term {
			break
		}
	}
	return i
}

// since returns a copy of the entries with absolute index >= i,
// for sending in AppendEntries arguments. i must be <= lastIndex();
// if it is not, nil is returned.
func (l *raftLog) since(i int) []LogEntry {
	rel := i - l.snapIndex
	if rel < 0 {
		rel = 0
	}
	if rel >= len(l.entries) {
		return nil
	}
	out := make([]LogEntry, len(l.entries)-rel)
	copy(out, l.entries[rel:])
	return out
}

// persistEntries exposes the raw slice for persistence encoding.
// The caller must not mutate it.
func (l *raftLog) persistEntries() []LogEntry {
	return l.entries
}
