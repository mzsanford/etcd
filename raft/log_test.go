package raft

import (
	"reflect"
	"testing"

	pb "github.com/coreos/etcd/raft/raftpb"
)

// TestAppend ensures:
// 1. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it
// 2.Append any new entries not already in the log
func TestAppend(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1}, {Term: 2}}
	previousUnstable := uint64(3)
	tests := []struct {
		after     uint64
		ents      []pb.Entry
		windex    uint64
		wents     []pb.Entry
		wunstable uint64
	}{
		{
			2,
			[]pb.Entry{},
			2,
			[]pb.Entry{{Term: 1}, {Term: 2}},
			3,
		},
		{
			2,
			[]pb.Entry{{Term: 2}},
			3,
			[]pb.Entry{{Term: 1}, {Term: 2}, {Term: 2}},
			3,
		},
		// conflicts with index 1
		{
			0,
			[]pb.Entry{{Term: 2}},
			1,
			[]pb.Entry{{Term: 2}},
			1,
		},
		// conflicts with index 2
		{
			1,
			[]pb.Entry{{Term: 3}, {Term: 3}},
			3,
			[]pb.Entry{{Term: 1}, {Term: 3}, {Term: 3}},
			2,
		},
	}

	for i, tt := range tests {
		raftLog := newLog()
		raftLog.ents = append(raftLog.ents, previousEnts...)
		raftLog.unstable = previousUnstable
		index := raftLog.append(tt.after, tt.ents...)
		if index != tt.windex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, index, tt.windex)
		}
		if g := raftLog.entries(1); !reflect.DeepEqual(g, tt.wents) {
			t.Errorf("#%d: logEnts = %+v, want %+v", i, g, tt.wents)
		}
		if g := raftLog.unstable; g != tt.wunstable {
			t.Errorf("#%d: unstable = %d, want %d", i, g, tt.wunstable)
		}
	}
}

// TestCompactionSideEffects ensures that all the log related funcationality works correctly after
// a compaction.
func TestCompactionSideEffects(t *testing.T) {
	var i uint64
	lastIndex := uint64(1000)
	lastTerm := lastIndex
	raftLog := newLog()

	for i = 0; i < lastIndex; i++ {
		raftLog.append(uint64(i), pb.Entry{Term: uint64(i + 1), Index: uint64(i + 1)})
	}
	raftLog.maybeCommit(lastIndex, lastTerm)
	raftLog.resetNextEnts()

	raftLog.compact(500)

	if raftLog.lastIndex() != lastIndex {
		t.Errorf("lastIndex = %d, want %d", raftLog.lastIndex(), lastIndex)
	}

	for i := raftLog.offset; i <= raftLog.lastIndex(); i++ {
		if raftLog.term(i) != i {
			t.Errorf("term(%d) = %d, want %d", i, raftLog.term(i), i)
		}
	}

	for i := raftLog.offset; i <= raftLog.lastIndex(); i++ {
		if !raftLog.matchTerm(i, i) {
			t.Errorf("matchTerm(%d) = false, want true", i)
		}
	}

	unstableEnts := raftLog.unstableEnts()
	if g := len(unstableEnts); g != 500 {
		t.Errorf("len(unstableEntries) = %d, want = %d", g, 500)
	}
	if unstableEnts[0].Index != 501 {
		t.Errorf("Index = %d, want = %d", unstableEnts[0].Index, 501)
	}

	prev := raftLog.lastIndex()
	raftLog.append(raftLog.lastIndex(), pb.Entry{Term: raftLog.lastIndex() + 1})
	if raftLog.lastIndex() != prev+1 {
		t.Errorf("lastIndex = %d, want = %d", raftLog.lastIndex(), prev+1)
	}

	ents := raftLog.entries(raftLog.lastIndex())
	if len(ents) != 1 {
		t.Errorf("len(entries) = %d, want = %d", len(ents), 1)
	}
}

func TestUnstableEnts(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		unstable  uint64
		wents     []pb.Entry
		wunstable uint64
	}{
		{3, nil, 3},
		{1, previousEnts, 3},
	}

	for i, tt := range tests {
		raftLog := newLog()
		raftLog.ents = append(raftLog.ents, previousEnts...)
		raftLog.unstable = tt.unstable
		ents := raftLog.unstableEnts()
		raftLog.resetUnstable()
		if !reflect.DeepEqual(ents, tt.wents) {
			t.Errorf("#%d: unstableEnts = %+v, want %+v", i, ents, tt.wents)
		}
		if g := raftLog.unstable; g != tt.wunstable {
			t.Errorf("#%d: unstable = %d, want %d", i, g, tt.wunstable)
		}
	}
}

//TestCompaction ensures that the number of log entreis is correct after compactions.
func TestCompaction(t *testing.T) {
	tests := []struct {
		applied   uint64
		lastIndex uint64
		compact   []uint64
		wleft     []int
		wallow    bool
	}{
		// out of upper bound
		{1000, 1000, []uint64{1001}, []int{-1}, false},
		{1000, 1000, []uint64{300, 500, 800, 900}, []int{701, 501, 201, 101}, true},
		// out of lower bound
		{1000, 1000, []uint64{300, 299}, []int{701, -1}, false},
		{0, 1000, []uint64{1}, []int{-1}, false},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if tt.wallow == true {
						t.Errorf("%d: allow = %v, want %v", i, false, true)
					}
				}
			}()

			raftLog := newLog()
			for i := uint64(0); i < tt.lastIndex; i++ {
				raftLog.append(uint64(i), pb.Entry{})
			}
			raftLog.maybeCommit(tt.applied, 0)
			raftLog.resetNextEnts()

			for j := 0; j < len(tt.compact); j++ {
				raftLog.compact(tt.compact[j])
				if len(raftLog.ents) != tt.wleft[j] {
					t.Errorf("#%d.%d len = %d, want %d", i, j, len(raftLog.ents), tt.wleft[j])
				}
			}
		}()
	}
}

func TestLogRestore(t *testing.T) {
	var i uint64
	raftLog := newLog()
	for i = 0; i < 100; i++ {
		raftLog.append(i, pb.Entry{Term: i + 1})
	}

	index := uint64(1000)
	term := uint64(1000)
	raftLog.restore(pb.Snapshot{Index: index, Term: term})

	// only has the guard entry
	if len(raftLog.ents) != 1 {
		t.Errorf("len = %d, want 0", len(raftLog.ents))
	}
	if raftLog.offset != index {
		t.Errorf("offset = %d, want %d", raftLog.offset, index)
	}
	if raftLog.applied != index {
		t.Errorf("applied = %d, want %d", raftLog.applied, index)
	}
	if raftLog.committed != index {
		t.Errorf("comitted = %d, want %d", raftLog.committed, index)
	}
	if raftLog.unstable != index+1 {
		t.Errorf("unstable = %d, want %d", raftLog.unstable, index+1)
	}
	if raftLog.term(index) != term {
		t.Errorf("term = %d, want %d", raftLog.term(index), term)
	}
}

func TestIsOutOfBounds(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)
	l := &raftLog{offset: offset, ents: make([]pb.Entry, num)}

	tests := []struct {
		index uint64
		w     bool
	}{
		{offset - 1, true},
		{offset, false},
		{offset + num/2, false},
		{offset + num - 1, false},
		{offset + num, true},
	}

	for i, tt := range tests {
		g := l.isOutOfBounds(tt.index)
		if g != tt.w {
			t.Errorf("#%d: isOutOfBounds = %v, want %v", i, g, tt.w)
		}
	}
}

func TestAt(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)

	l := &raftLog{offset: offset}
	for i = 0; i < num; i++ {
		l.ents = append(l.ents, pb.Entry{Term: i})
	}

	tests := []struct {
		index uint64
		w     *pb.Entry
	}{
		{offset - 1, nil},
		{offset, &pb.Entry{Term: 0}},
		{offset + num/2, &pb.Entry{Term: num / 2}},
		{offset + num - 1, &pb.Entry{Term: num - 1}},
		{offset + num, nil},
	}

	for i, tt := range tests {
		g := l.at(tt.index)
		if !reflect.DeepEqual(g, tt.w) {
			t.Errorf("#%d: at = %v, want %v", i, g, tt.w)
		}
	}
}

func TestSlice(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)

	l := &raftLog{offset: offset}
	for i = 0; i < num; i++ {
		l.ents = append(l.ents, pb.Entry{Term: i})
	}

	tests := []struct {
		from uint64
		to   uint64
		w    []pb.Entry
	}{
		{offset - 1, offset + 1, nil},
		{offset, offset + 1, []pb.Entry{{Term: 0}}},
		{offset + num/2, offset + num/2 + 1, []pb.Entry{{Term: num / 2}}},
		{offset + num - 1, offset + num, []pb.Entry{{Term: num - 1}}},
		{offset + num, offset + num + 1, nil},

		{offset + num/2, offset + num/2, nil},
		{offset + num/2, offset + num/2 - 1, nil},
	}

	for i, tt := range tests {
		g := l.slice(tt.from, tt.to)
		if !reflect.DeepEqual(g, tt.w) {
			t.Errorf("#%d: from %d to %d = %v, want %v", i, tt.from, tt.to, g, tt.w)
		}
	}
}
