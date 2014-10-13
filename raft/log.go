package raft

import (
	"fmt"

	pb "github.com/coreos/etcd/raft/raftpb"
)

const (
	defaultCompactThreshold = 10000
)

type raftLog struct {
	ents      []pb.Entry
	unstable  uint64
	committed uint64
	applied   uint64
	offset    uint64
	snapshot  pb.Snapshot

	// want a compact after the number of entries exceeds the threshold
	// TODO(xiangli) size might be a better criteria
	compactThreshold uint64
}

func newLog() *raftLog {
	return &raftLog{
		ents:             make([]pb.Entry, 1),
		unstable:         0,
		committed:        0,
		applied:          0,
		compactThreshold: defaultCompactThreshold,
	}
}

func (l *raftLog) isEmpty() bool {
	return l.offset == 0 && len(l.ents) == 1
}

func (l *raftLog) load(ents []pb.Entry) {
	l.ents = ents
	l.unstable = l.offset + uint64(len(ents))
}

func (l *raftLog) String() string {
	return fmt.Sprintf("offset=%d committed=%d applied=%d len(ents)=%d", l.offset, l.committed, l.applied, len(l.ents))
}

func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) bool {
	lastnewi := index + uint64(len(ents))
	if l.matchTerm(index, logTerm) {
		from := index + 1
		ci := l.findConflict(from, ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			panic("conflict with committed entry")
		default:
			l.append(ci-1, ents[ci-from:]...)
		}
		tocommit := min(committed, lastnewi)
		// if toCommit > commitIndex, set commitIndex = toCommit
		if l.committed < tocommit {
			l.committed = tocommit
		}
		return true
	}
	return false
}

func (l *raftLog) append(after uint64, ents ...pb.Entry) uint64 {
	l.ents = append(l.slice(l.offset, after+1), ents...)
	l.unstable = min(l.unstable, after+1)
	return l.lastIndex()
}

func (l *raftLog) findConflict(from uint64, ents []pb.Entry) uint64 {
	for i, ne := range ents {
		if oe := l.at(from + uint64(i)); oe == nil || oe.Term != ne.Term {
			return from + uint64(i)
		}
	}
	return 0
}

func (l *raftLog) unstableEnts() []pb.Entry {
	ents := l.slice(l.unstable, l.lastIndex()+1)
	if ents == nil {
		return nil
	}
	cpy := make([]pb.Entry, len(ents))
	copy(cpy, ents)
	return cpy
}

func (l *raftLog) resetUnstable() {
	l.unstable = l.lastIndex() + 1
}

// nextEnts returns all the available entries for execution.
// all the returned entries will be marked as applied.
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	if l.committed > l.applied {
		return l.slice(l.applied+1, l.committed+1)
	}
	return nil
}

func (l *raftLog) resetNextEnts() {
	if l.committed > l.applied {
		l.applied = l.committed
	}
}

func (l *raftLog) lastIndex() uint64 {
	return uint64(len(l.ents)) - 1 + l.offset
}

func (l *raftLog) term(i uint64) uint64 {
	if e := l.at(i); e != nil {
		return e.Term
	}
	return 0
}

func (l *raftLog) entries(i uint64) []pb.Entry {
	// never send out the first entry
	// first entry is only used for matching
	// prevLogTerm
	if i == l.offset {
		panic("cannot return the first entry in log")
	}
	return l.slice(i, l.lastIndex()+1)
}

func (l *raftLog) isUpToDate(i, term uint64) bool {
	e := l.at(l.lastIndex())
	return term > e.Term || (term == e.Term && i >= l.lastIndex())
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	if e := l.at(i); e != nil {
		return e.Term == term
	}
	return false
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.term(maxIndex) == term {
		l.committed = maxIndex
		return true
	}
	return false
}

// compact compacts all log entries until i.
// It removes the log entries before i, exclusive.
// i must be not smaller than the index of the first entry
// and not greater than the index of the last entry.
// the number of entries after compaction will be returned.
func (l *raftLog) compact(i uint64) uint64 {
	if l.isOutOfAppliedBounds(i) {
		panic(fmt.Sprintf("compact %d out of bounds [%d:%d]", i, l.offset, l.applied))
	}
	l.ents = l.slice(i, l.lastIndex()+1)
	l.unstable = max(i+1, l.unstable)
	l.offset = i
	return uint64(len(l.ents))
}

func (l *raftLog) snap(d []byte, index, term uint64, nodes []uint64, removed []uint64) {
	l.snapshot = pb.Snapshot{
		Data:         d,
		Nodes:        nodes,
		Index:        index,
		Term:         term,
		RemovedNodes: removed,
	}
}

func (l *raftLog) shouldCompact() bool {
	return (l.applied - l.offset) > l.compactThreshold
}

func (l *raftLog) restore(s pb.Snapshot) {
	l.ents = []pb.Entry{{Term: s.Term}}
	l.unstable = s.Index + 1
	l.committed = s.Index
	l.applied = s.Index
	l.offset = s.Index
	l.snapshot = s
}

func (l *raftLog) at(i uint64) *pb.Entry {
	if l.isOutOfBounds(i) {
		return nil
	}
	return &l.ents[i-l.offset]
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *raftLog) slice(lo uint64, hi uint64) []pb.Entry {
	if lo >= hi {
		return nil
	}
	if l.isOutOfBounds(lo) || l.isOutOfBounds(hi-1) {
		return nil
	}
	return l.ents[lo-l.offset : hi-l.offset]
}

func (l *raftLog) isOutOfBounds(i uint64) bool {
	if i < l.offset || i > l.lastIndex() {
		return true
	}
	return false
}

func (l *raftLog) isOutOfAppliedBounds(i uint64) bool {
	if i < l.offset || i > l.applied {
		return true
	}
	return false
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
