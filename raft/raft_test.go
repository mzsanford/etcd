package raft

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	pb "github.com/coreos/etcd/raft/raftpb"
)

// nextEnts returns the appliable entries and updates the applied index
func nextEnts(r *raft) (ents []pb.Entry) {
	ents = r.raftLog.nextEnts()
	r.raftLog.resetNextEnts()
	return ents
}

type Interface interface {
	Step(m pb.Message) error
	ReadMessages() []pb.Message
}

func TestLeaderElection(t *testing.T) {
	tests := []struct {
		*network
		state StateType
	}{
		{newNetwork(nil, nil, nil), StateLeader},
		{newNetwork(nil, nil, nopStepper), StateLeader},
		{newNetwork(nil, nopStepper, nopStepper), StateCandidate},
		{newNetwork(nil, nopStepper, nopStepper, nil), StateCandidate},
		{newNetwork(nil, nopStepper, nopStepper, nil, nil), StateLeader},

		// three logs further along than 0
		{newNetwork(nil, ents(1), ents(2), ents(1, 3), nil), StateFollower},

		// logs converge
		{newNetwork(ents(1), nil, ents(2), ents(1), nil), StateLeader},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
		sm := tt.network.peers[1].(*raft)
		if sm.state != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, sm.state, tt.state)
		}
		if g := sm.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

func TestLogReplication(t *testing.T) {
	tests := []struct {
		*network
		msgs       []pb.Message
		wcommitted uint64
	}{
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
			},
			2,
		},
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
				{From: 1, To: 2, Type: msgHup},
				{From: 1, To: 2, Type: msgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
			},
			4,
		},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

		for _, m := range tt.msgs {
			tt.send(m)
		}

		for j, x := range tt.network.peers {
			sm := x.(*raft)

			if sm.raftLog.committed != tt.wcommitted {
				t.Errorf("#%d.%d: committed = %d, want %d", i, j, sm.raftLog.committed, tt.wcommitted)
			}

			ents := []pb.Entry{}
			for _, e := range nextEnts(sm) {
				if e.Data != nil {
					ents = append(ents, e)
				}
			}
			props := []pb.Message{}
			for _, m := range tt.msgs {
				if m.Type == msgProp {
					props = append(props, m)
				}
			}
			for k, m := range props {
				if !bytes.Equal(ents[k].Data, m.Entries[0].Data) {
					t.Errorf("#%d.%d: data = %d, want %d", i, j, ents[k].Data, m.Entries[0].Data)
				}
			}
		}
	}
}

func TestSingleNodeCommit(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	if sm.raftLog.committed != 3 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 3)
	}
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
func TestCannotCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	if sm.raftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 1)
	}

	// network recovery
	tt.recover()
	// avoid committing ChangeTerm proposal
	tt.ignore(msgApp)

	// elect 1 as the new leader with term 2
	tt.send(pb.Message{From: 2, To: 2, Type: msgHup})

	// no log entries from previous term should be committed
	sm = tt.peers[2].(*raft)
	if sm.raftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 1)
	}

	tt.recover()

	// still be able to append a entry
	tt.send(pb.Message{From: 2, To: 2, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	if sm.raftLog.committed != 5 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 5)
	}
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.
func TestCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	if sm.raftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 1)
	}

	// network recovery
	tt.recover()

	// elect 1 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	tt.send(pb.Message{From: 2, To: 2, Type: msgHup})

	if sm.raftLog.committed != 4 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 4)
	}
}

func TestDuelingCandidates(t *testing.T) {
	a := newRaft(1, []uint64{1, 2, 3}, 10, 1)
	b := newRaft(2, []uint64{1, 2, 3}, 10, 1)
	c := newRaft(3, []uint64{1, 2, 3}, 10, 1)

	nt := newNetwork(a, b, c)
	nt.cut(1, 3)

	nt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	nt.send(pb.Message{From: 3, To: 3, Type: msgHup})

	nt.recover()
	nt.send(pb.Message{From: 3, To: 3, Type: msgHup})

	wlog := &raftLog{ents: []pb.Entry{{}, pb.Entry{Data: nil, Term: 1, Index: 1}}, committed: 1}
	tests := []struct {
		sm      *raft
		state   StateType
		term    uint64
		raftLog *raftLog
	}{
		{a, StateFollower, 2, wlog},
		{b, StateFollower, 2, wlog},
		{c, StateFollower, 2, newLog()},
	}

	for i, tt := range tests {
		if g := tt.sm.state; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.Term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}
		base := ltoa(tt.raftLog)
		if sm, ok := nt.peers[1+uint64(i)].(*raft); ok {
			l := ltoa(sm.raftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestCandidateConcede(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	tt.isolate(1)

	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	tt.send(pb.Message{From: 3, To: 3, Type: msgHup})

	// heal the partition
	tt.recover()

	data := []byte("force follower")
	// send a proposal to 2 to flush out a msgApp to 0
	tt.send(pb.Message{From: 3, To: 3, Type: msgProp, Entries: []pb.Entry{{Data: data}}})

	a := tt.peers[1].(*raft)
	if g := a.state; g != StateFollower {
		t.Errorf("state = %s, want %s", g, StateFollower)
	}
	if g := a.Term; g != 1 {
		t.Errorf("term = %d, want %d", g, 1)
	}
	wantLog := ltoa(&raftLog{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}}, committed: 2})
	for i, p := range tt.peers {
		if sm, ok := p.(*raft); ok {
			l := ltoa(sm.raftLog)
			if g := diffu(wantLog, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestSingleNodeCandidate(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

	sm := tt.peers[1].(*raft)
	if sm.state != StateLeader {
		t.Errorf("state = %d, want %d", sm.state, StateLeader)
	}
}

func TestOldMessages(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	// make 0 leader @ term 3
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	tt.send(pb.Message{From: 2, To: 2, Type: msgHup})
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	// pretend we're an old leader trying to make progress
	tt.send(pb.Message{From: 1, To: 1, Type: msgApp, Term: 1, Entries: []pb.Entry{{Term: 1}}})

	l := &raftLog{
		ents: []pb.Entry{
			{}, {Data: nil, Term: 1, Index: 1},
			{Data: nil, Term: 2, Index: 2}, {Data: nil, Term: 3, Index: 3},
		},
		committed: 3,
	}
	base := ltoa(l)
	for i, p := range tt.peers {
		if sm, ok := p.(*raft); ok {
			l := ltoa(sm.raftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

// TestOldMessagesReply - optimization - reply with new term.

func TestProposal(t *testing.T) {
	tests := []struct {
		*network
		success bool
	}{
		{newNetwork(nil, nil, nil), true},
		{newNetwork(nil, nil, nopStepper), true},
		{newNetwork(nil, nopStepper, nopStepper), false},
		{newNetwork(nil, nopStepper, nopStepper, nil), false},
		{newNetwork(nil, nopStepper, nopStepper, nil, nil), true},
	}

	for i, tt := range tests {
		send := func(m pb.Message) {
			defer func() {
				// only recover is we expect it to panic so
				// panics we don't expect go up.
				if !tt.success {
					e := recover()
					if e != nil {
						t.Logf("#%d: err: %s", i, e)
					}
				}
			}()
			tt.send(m)
		}

		data := []byte("somedata")

		// promote 0 the leader
		send(pb.Message{From: 1, To: 1, Type: msgHup})
		send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: data}}})

		wantLog := newLog()
		if tt.success {
			wantLog = &raftLog{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}}, committed: 2}
		}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*raft); ok {
				l := ltoa(sm.raftLog)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}
		sm := tt.network.peers[1].(*raft)
		if g := sm.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

func TestProposalByProxy(t *testing.T) {
	data := []byte("somedata")
	tests := []*network{
		newNetwork(nil, nil, nil),
		newNetwork(nil, nil, nopStepper),
	}

	for i, tt := range tests {
		// promote 0 the leader
		tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

		// propose via follower
		tt.send(pb.Message{From: 2, To: 2, Type: msgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

		wantLog := &raftLog{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Data: data, Index: 2}}, committed: 2}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*raft); ok {
				l := ltoa(sm.raftLog)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}
		sm := tt.peers[1].(*raft)
		if g := sm.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

func TestCompact(t *testing.T) {
	tests := []struct {
		compacti uint64
		nodes    []uint64
		removed  []uint64
		snapd    []byte
		wpanic   bool
	}{
		{1, []uint64{1, 2, 3}, []uint64{4, 5}, []byte("some data"), false},
		{2, []uint64{1, 2, 3}, []uint64{4, 5}, []byte("some data"), false},
		{4, []uint64{1, 2, 3}, []uint64{4, 5}, []byte("some data"), true}, // compact out of range
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if tt.wpanic != true {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			sm := &raft{
				state: StateLeader,
				raftLog: &raftLog{
					committed: 2,
					applied:   2,
					ents:      []pb.Entry{{}, {Term: 1}, {Term: 1}, {Term: 1}},
				},
				removed: make(map[uint64]bool),
			}
			for _, r := range tt.removed {
				sm.removeNode(r)
			}
			sm.compact(tt.compacti, tt.nodes, tt.snapd)
			sort.Sort(uint64Slice(sm.raftLog.snapshot.Nodes))
			sort.Sort(uint64Slice(sm.raftLog.snapshot.RemovedNodes))
			if sm.raftLog.offset != tt.compacti {
				t.Errorf("%d: log.offset = %d, want %d", i, sm.raftLog.offset, tt.compacti)
			}
			if !reflect.DeepEqual(sm.raftLog.snapshot.Nodes, tt.nodes) {
				t.Errorf("%d: snap.nodes = %v, want %v", i, sm.raftLog.snapshot.Nodes, tt.nodes)
			}
			if !reflect.DeepEqual(sm.raftLog.snapshot.Data, tt.snapd) {
				t.Errorf("%d: snap.data = %v, want %v", i, sm.raftLog.snapshot.Data, tt.snapd)
			}
			if !reflect.DeepEqual(sm.raftLog.snapshot.RemovedNodes, tt.removed) {
				t.Errorf("%d: snap.removedNodes = %v, want %v", i, sm.raftLog.snapshot.RemovedNodes, tt.removed)
			}
		}()
	}
}

func TestCommit(t *testing.T) {
	tests := []struct {
		matches []uint64
		logs    []pb.Entry
		smTerm  uint64
		w       uint64
	}{
		// single
		{[]uint64{1}, []pb.Entry{{}, {Term: 1}}, 1, 1},
		{[]uint64{1}, []pb.Entry{{}, {Term: 1}}, 2, 0},
		{[]uint64{2}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 2, 2},
		{[]uint64{1}, []pb.Entry{{}, {Term: 2}}, 2, 1},

		// odd
		{[]uint64{2, 1, 1}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
		{[]uint64{2, 1, 2}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 2, 2},
		{[]uint64{2, 1, 2}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},

		// even
		{[]uint64{2, 1, 1, 1}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1, 1}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
		{[]uint64{2, 1, 1, 2}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 1, 1},
		{[]uint64{2, 1, 1, 2}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
		{[]uint64{2, 1, 2, 2}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 2, 2},
		{[]uint64{2, 1, 2, 2}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
	}

	for i, tt := range tests {
		prs := make(map[uint64]*progress)
		for j := 0; j < len(tt.matches); j++ {
			prs[uint64(j)] = &progress{tt.matches[j], tt.matches[j] + 1}
		}
		sm := &raft{raftLog: &raftLog{ents: tt.logs}, prs: prs, HardState: pb.HardState{Term: tt.smTerm}}
		sm.maybeCommit()
		if g := sm.raftLog.committed; g != tt.w {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.w)
		}
	}
}

func TestIsElectionTimeout(t *testing.T) {
	tests := []struct {
		elapse       int
		wprobability float64
		round        bool
	}{
		{5, 0, false},
		{13, 0.3, true},
		{15, 0.5, true},
		{18, 0.8, true},
		{20, 1, false},
	}

	for i, tt := range tests {
		sm := newRaft(1, []uint64{1}, 10, 1)
		sm.elapsed = tt.elapse
		c := 0
		for j := 0; j < 10000; j++ {
			if sm.isElectionTimeout() {
				c++
			}
		}
		got := float64(c) / 10000.0
		if tt.round {
			got = math.Floor(got*10+0.5) / 10.0
		}
		if got != tt.wprobability {
			t.Errorf("#%d: possibility = %v, want %v", i, got, tt.wprobability)
		}
	}
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// acutal stepX function.
func TestStepIgnoreOldTermMsg(t *testing.T) {
	called := false
	fakeStep := func(r *raft, m pb.Message) {
		called = true
	}
	sm := newRaft(1, []uint64{1}, 10, 1)
	sm.step = fakeStep
	sm.Term = 2
	sm.Step(pb.Message{Type: msgApp, Term: sm.Term - 1})
	if called == true {
		t.Errorf("stepFunc called = %v , want %v", called, false)
	}
}

// TestHandleMsgApp ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
func TestHandleMsgApp(t *testing.T) {
	tests := []struct {
		m       pb.Message
		wIndex  uint64
		wCommit uint64
		wReject bool
	}{
		// Ensure 1
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 3, Index: 2, Commit: 3}, 2, 0, true}, // previous log mismatch
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 3, Index: 3, Commit: 3}, 2, 0, true}, // previous log non-exist

		// Ensure 2
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 1, Index: 1, Commit: 1}, 2, 1, false},
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 0, Index: 0, Commit: 1, Entries: []pb.Entry{{Term: 2}}}, 1, 1, false},
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 3, Entries: []pb.Entry{{Term: 2}, {Term: 2}}}, 4, 3, false},
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 4, Entries: []pb.Entry{{Term: 2}}}, 3, 3, false},
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 1, Index: 1, Commit: 4, Entries: []pb.Entry{{Term: 2}}}, 2, 2, false},

		// Ensure 3
		{pb.Message{Type: msgApp, Term: 1, LogTerm: 1, Index: 1, Commit: 3}, 2, 1, false},                                 // match entry 1, commit upto last new entry 1
		{pb.Message{Type: msgApp, Term: 1, LogTerm: 1, Index: 1, Commit: 3, Entries: []pb.Entry{{Term: 2}}}, 2, 2, false}, // match entry 1, commit upto last new entry 2
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 3}, 2, 2, false},                                 // match entry 2, commit upto last new entry 2
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 4}, 2, 2, false},                                 // commit upto log.last()
	}

	for i, tt := range tests {
		sm := &raft{
			state:     StateFollower,
			HardState: pb.HardState{Term: 2},
			raftLog:   &raftLog{committed: 0, ents: []pb.Entry{{}, {Term: 1}, {Term: 2}}},
		}

		sm.handleAppendEntries(tt.m)
		if sm.raftLog.lastIndex() != tt.wIndex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, sm.raftLog.lastIndex(), tt.wIndex)
		}
		if sm.raftLog.committed != tt.wCommit {
			t.Errorf("#%d: committed = %d, want %d", i, sm.raftLog.committed, tt.wCommit)
		}
		m := sm.ReadMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: msg = nil, want 1", i)
		}
		if m[0].Reject != tt.wReject {
			t.Errorf("#%d: reject = %v, want %v", i, m[0].Reject, tt.wReject)
		}
	}
}

func TestRecvMsgVote(t *testing.T) {
	tests := []struct {
		state   StateType
		i, term uint64
		voteFor uint64
		wreject bool
	}{
		{StateFollower, 0, 0, None, true},
		{StateFollower, 0, 1, None, true},
		{StateFollower, 0, 2, None, true},
		{StateFollower, 0, 3, None, false},

		{StateFollower, 1, 0, None, true},
		{StateFollower, 1, 1, None, true},
		{StateFollower, 1, 2, None, true},
		{StateFollower, 1, 3, None, false},

		{StateFollower, 2, 0, None, true},
		{StateFollower, 2, 1, None, true},
		{StateFollower, 2, 2, None, false},
		{StateFollower, 2, 3, None, false},

		{StateFollower, 3, 0, None, true},
		{StateFollower, 3, 1, None, true},
		{StateFollower, 3, 2, None, false},
		{StateFollower, 3, 3, None, false},

		{StateFollower, 3, 2, 2, false},
		{StateFollower, 3, 2, 1, true},

		{StateLeader, 3, 3, 1, true},
		{StateCandidate, 3, 3, 1, true},
	}

	for i, tt := range tests {
		sm := newRaft(1, []uint64{1}, 10, 1)
		sm.state = tt.state
		switch tt.state {
		case StateFollower:
			sm.step = stepFollower
		case StateCandidate:
			sm.step = stepCandidate
		case StateLeader:
			sm.step = stepLeader
		}
		sm.HardState = pb.HardState{Vote: tt.voteFor}
		sm.raftLog = &raftLog{ents: []pb.Entry{{}, {Term: 2}, {Term: 2}}}

		sm.Step(pb.Message{Type: msgVote, From: 2, Index: tt.i, LogTerm: tt.term})

		msgs := sm.ReadMessages()
		if g := len(msgs); g != 1 {
			t.Fatalf("#%d: len(msgs) = %d, want 1", i, g)
			continue
		}
		if g := msgs[0].Reject; g != tt.wreject {
			t.Errorf("#%d, m.Reject = %v, want %v", i, g, tt.wreject)
		}
	}
}

func TestStateTransition(t *testing.T) {
	tests := []struct {
		from   StateType
		to     StateType
		wallow bool
		wterm  uint64
		wlead  uint64
	}{
		{StateFollower, StateFollower, true, 1, None},
		{StateFollower, StateCandidate, true, 1, None},
		{StateFollower, StateLeader, false, 0, None},

		{StateCandidate, StateFollower, true, 0, None},
		{StateCandidate, StateCandidate, true, 1, None},
		{StateCandidate, StateLeader, true, 0, 1},

		{StateLeader, StateFollower, true, 1, None},
		{StateLeader, StateCandidate, false, 1, None},
		{StateLeader, StateLeader, true, 0, 1},
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

			sm := newRaft(1, []uint64{1}, 10, 1)
			sm.state = tt.from

			switch tt.to {
			case StateFollower:
				sm.becomeFollower(tt.wterm, tt.wlead)
			case StateCandidate:
				sm.becomeCandidate()
			case StateLeader:
				sm.becomeLeader()
			}

			if sm.Term != tt.wterm {
				t.Errorf("%d: term = %d, want %d", i, sm.Term, tt.wterm)
			}
			if sm.lead != tt.wlead {
				t.Errorf("%d: lead = %d, want %d", i, sm.lead, tt.wlead)
			}
		}()
	}
}

func TestAllServerStepdown(t *testing.T) {
	tests := []struct {
		state StateType

		wstate StateType
		wterm  uint64
		windex uint64
	}{
		{StateFollower, StateFollower, 3, 1},
		{StateCandidate, StateFollower, 3, 1},
		{StateLeader, StateFollower, 3, 2},
	}

	tmsgTypes := [...]uint64{msgVote, msgApp}
	tterm := uint64(3)

	for i, tt := range tests {
		sm := newRaft(1, []uint64{1, 2, 3}, 10, 1)
		switch tt.state {
		case StateFollower:
			sm.becomeFollower(1, None)
		case StateCandidate:
			sm.becomeCandidate()
		case StateLeader:
			sm.becomeCandidate()
			sm.becomeLeader()
		}

		for j, msgType := range tmsgTypes {
			sm.Step(pb.Message{From: 2, Type: msgType, Term: tterm, LogTerm: tterm})

			if sm.state != tt.wstate {
				t.Errorf("#%d.%d state = %v , want %v", i, j, sm.state, tt.wstate)
			}
			if sm.Term != tt.wterm {
				t.Errorf("#%d.%d term = %v , want %v", i, j, sm.Term, tt.wterm)
			}
			if uint64(len(sm.raftLog.ents)) != tt.windex {
				t.Errorf("#%d.%d index = %v , want %v", i, j, len(sm.raftLog.ents), tt.windex)
			}
			wlead := uint64(2)
			if msgType == msgVote {
				wlead = None
			}
			if sm.lead != wlead {
				t.Errorf("#%d, sm.lead = %d, want %d", i, sm.lead, None)
			}
		}
	}
}

func TestLeaderAppResp(t *testing.T) {
	tests := []struct {
		index      uint64
		reject     bool
		wmsgNum    int
		windex     uint64
		wcommitted uint64
	}{
		{3, true, 0, 0, 0},  // stale resp; no replies
		{2, true, 1, 1, 0},  // denied resp; leader does not commit; decrese next and send probing msg
		{2, false, 2, 2, 2}, // accept resp; leader commits; broadcast with commit index
	}

	for i, tt := range tests {
		// sm term is 1 after it becomes the leader.
		// thus the last log term must be 1 to be committed.
		sm := newRaft(1, []uint64{1, 2, 3}, 10, 1)
		sm.raftLog = &raftLog{ents: []pb.Entry{{}, {Term: 0}, {Term: 1}}}
		sm.becomeCandidate()
		sm.becomeLeader()
		sm.ReadMessages()
		sm.Step(pb.Message{From: 2, Type: msgAppResp, Index: tt.index, Term: sm.Term, Reject: tt.reject})
		msgs := sm.ReadMessages()

		if len(msgs) != tt.wmsgNum {
			t.Errorf("#%d msgNum = %d, want %d", i, len(msgs), tt.wmsgNum)
		}
		for j, msg := range msgs {
			if msg.Index != tt.windex {
				t.Errorf("#%d.%d index = %d, want %d", i, j, msg.Index, tt.windex)
			}
			if msg.Commit != tt.wcommitted {
				t.Errorf("#%d.%d commit = %d, want %d", i, j, msg.Commit, tt.wcommitted)
			}
		}
	}
}

// When the leader receives a heartbeat tick, it should
// send a msgApp with m.Index = 0, m.LogTerm=0 and empty entries.
func TestBcastBeat(t *testing.T) {
	offset := uint64(1000)
	// make a state machine with log.offset = 1000
	s := pb.Snapshot{
		Index: offset,
		Term:  1,
		Nodes: []uint64{1, 2, 3},
	}
	sm := newRaft(1, []uint64{1, 2, 3}, 10, 1)
	sm.Term = 1
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()
	for i := 0; i < 10; i++ {
		sm.appendEntry(pb.Entry{})
	}

	sm.Step(pb.Message{Type: msgBeat})
	msgs := sm.ReadMessages()
	if len(msgs) != 2 {
		t.Fatalf("len(msgs) = %v, want 1", len(msgs))
	}
	tomap := map[uint64]bool{2: true, 3: true}
	for i, m := range msgs {
		if m.Type != msgApp {
			t.Fatalf("#%d: type = %v, want = %v", i, m.Type, msgApp)
		}
		if m.Index != 0 {
			t.Fatalf("#%d: prevIndex = %d, want %d", i, m.Index, 0)
		}
		if m.LogTerm != 0 {
			t.Fatalf("#%d: prevTerm = %d, want %d", i, m.LogTerm, 0)
		}
		if !tomap[m.To] {
			t.Fatalf("#%d: unexpected to %d", i, m.To)
		} else {
			delete(tomap, m.To)
		}
		if len(m.Entries) != 0 {
			t.Fatalf("#%d: len(entries) = %d, want 0", i, len(m.Entries))
		}
	}
}

// tests the output of the statemachine when receiving msgBeat
func TestRecvMsgBeat(t *testing.T) {
	tests := []struct {
		state StateType
		wMsg  int
	}{
		{StateLeader, 2},
		// candidate and follower should ignore msgBeat
		{StateCandidate, 0},
		{StateFollower, 0},
	}

	for i, tt := range tests {
		sm := newRaft(1, []uint64{1, 2, 3}, 10, 1)
		sm.raftLog = &raftLog{ents: []pb.Entry{{}, {Term: 0}, {Term: 1}}}
		sm.Term = 1
		sm.state = tt.state
		switch tt.state {
		case StateFollower:
			sm.step = stepFollower
		case StateCandidate:
			sm.step = stepCandidate
		case StateLeader:
			sm.step = stepLeader
		}
		sm.Step(pb.Message{From: 1, To: 1, Type: msgBeat})

		msgs := sm.ReadMessages()
		if len(msgs) != tt.wMsg {
			t.Errorf("%d: len(msgs) = %d, want %d", i, len(msgs), tt.wMsg)
		}
		for _, m := range msgs {
			if m.Type != msgApp {
				t.Errorf("%d: msg.type = %v, want %v", i, m.Type, msgApp)
			}
		}
	}
}

func TestRestore(t *testing.T) {
	s := pb.Snapshot{
		Index:        defaultCompactThreshold + 1,
		Term:         defaultCompactThreshold + 1,
		Nodes:        []uint64{1, 2, 3},
		RemovedNodes: []uint64{4, 5},
	}

	sm := newRaft(1, []uint64{1, 2}, 10, 1)
	if ok := sm.restore(s); !ok {
		t.Fatal("restore fail, want succeed")
	}

	if sm.raftLog.lastIndex() != s.Index {
		t.Errorf("log.lastIndex = %d, want %d", sm.raftLog.lastIndex(), s.Index)
	}
	if sm.raftLog.term(s.Index) != s.Term {
		t.Errorf("log.lastTerm = %d, want %d", sm.raftLog.term(s.Index), s.Term)
	}
	sg := sm.nodes()
	srn := sm.removedNodes()
	sort.Sort(uint64Slice(sg))
	sort.Sort(uint64Slice(srn))
	if !reflect.DeepEqual(sg, s.Nodes) {
		t.Errorf("sm.Nodes = %+v, want %+v", sg, s.Nodes)
	}
	if !reflect.DeepEqual(s.RemovedNodes, srn) {
		t.Errorf("sm.RemovedNodes = %+v, want %+v", s.RemovedNodes, srn)
	}
	if !reflect.DeepEqual(sm.raftLog.snapshot, s) {
		t.Errorf("snapshot = %+v, want %+v", sm.raftLog.snapshot, s)
	}

	if ok := sm.restore(s); ok {
		t.Fatal("restore succeed, want fail")
	}
}

func TestProvideSnap(t *testing.T) {
	s := pb.Snapshot{
		Index: defaultCompactThreshold + 1,
		Term:  defaultCompactThreshold + 1,
		Nodes: []uint64{1, 2},
	}
	sm := newRaft(1, []uint64{1}, 10, 1)
	// restore the statemachin from a snapshot
	// so it has a compacted log and a snapshot
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()

	// force set the next of node 1, so that
	// node 1 needs a snapshot
	sm.prs[2].next = sm.raftLog.offset

	sm.Step(pb.Message{From: 2, To: 1, Type: msgAppResp, Index: sm.prs[2].next - 1, Reject: true})
	msgs := sm.ReadMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Type != msgSnap {
		t.Errorf("m.Type = %v, want %v", m.Type, msgSnap)
	}
}

func TestRestoreFromSnapMsg(t *testing.T) {
	s := pb.Snapshot{
		Index: defaultCompactThreshold + 1,
		Term:  defaultCompactThreshold + 1,
		Nodes: []uint64{1, 2},
	}
	m := pb.Message{Type: msgSnap, From: 1, Term: 2, Snapshot: s}

	sm := newRaft(2, []uint64{1, 2}, 10, 1)
	sm.Step(m)

	if !reflect.DeepEqual(sm.raftLog.snapshot, s) {
		t.Errorf("snapshot = %+v, want %+v", sm.raftLog.snapshot, s)
	}
}

func TestSlowNodeRestore(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: msgHup})

	nt.isolate(3)
	for j := 0; j < defaultCompactThreshold+1; j++ {
		nt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{}}})
	}
	lead := nt.peers[1].(*raft)
	nextEnts(lead)
	lead.compact(lead.raftLog.applied, lead.nodes(), nil)

	nt.recover()
	// trigger a snapshot
	nt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{}}})
	follower := nt.peers[3].(*raft)
	if !reflect.DeepEqual(follower.raftLog.snapshot, lead.raftLog.snapshot) {
		t.Errorf("follower.snap = %+v, want %+v", follower.raftLog.snapshot, lead.raftLog.snapshot)
	}

	// trigger a commit
	nt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{}}})
	if follower.raftLog.committed != lead.raftLog.committed {
		t.Errorf("follower.comitted = %d, want %d", follower.raftLog.committed, lead.raftLog.committed)
	}
}

// TestStepConfig tests that when raft step msgProp in EntryConfChange type,
// it appends the entry to log and sets pendingConf to be true.
func TestStepConfig(t *testing.T) {
	// a raft that cannot make progress
	r := newRaft(1, []uint64{1, 2}, 10, 1)
	r.becomeCandidate()
	r.becomeLeader()
	index := r.raftLog.lastIndex()
	r.Step(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange}}})
	if g := r.raftLog.lastIndex(); g != index+1 {
		t.Errorf("index = %d, want %d", g, index+1)
	}
	if r.pendingConf != true {
		t.Errorf("pendingConf = %v, want true", r.pendingConf)
	}
}

// TestStepIgnoreConfig tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will deny
// the proposal and keep its original state.
func TestStepIgnoreConfig(t *testing.T) {
	// a raft that cannot make progress
	r := newRaft(1, []uint64{1, 2}, 10, 1)
	r.becomeCandidate()
	r.becomeLeader()
	r.Step(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange}}})
	index := r.raftLog.lastIndex()
	pendingConf := r.pendingConf
	r.Step(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange}}})
	if g := r.raftLog.lastIndex(); g != index {
		t.Errorf("index = %d, want %d", g, index)
	}
	if r.pendingConf != pendingConf {
		t.Errorf("pendingConf = %v, want %v", r.pendingConf, pendingConf)
	}
}

// TestRecoverPendingConfig tests that new leader recovers its pendingConf flag
// based on uncommitted entries.
func TestRecoverPendingConfig(t *testing.T) {
	tests := []struct {
		entType  pb.EntryType
		wpending bool
	}{
		{pb.EntryNormal, false},
		{pb.EntryConfChange, true},
	}
	for i, tt := range tests {
		r := newRaft(1, []uint64{1, 2}, 10, 1)
		r.appendEntry(pb.Entry{Type: tt.entType})
		r.becomeCandidate()
		r.becomeLeader()
		if r.pendingConf != tt.wpending {
			t.Errorf("#%d: pendingConf = %v, want %v", i, r.pendingConf, tt.wpending)
		}
	}
}

// TestRecoverDoublePendingConfig tests that new leader will panic if
// there exist two uncommitted config entries.
func TestRecoverDoublePendingConfig(t *testing.T) {
	func() {
		defer func() {
			if err := recover(); err == nil {
				t.Errorf("expect panic, but nothing happens")
			}
		}()
		r := newRaft(1, []uint64{1, 2}, 10, 1)
		r.appendEntry(pb.Entry{Type: pb.EntryConfChange})
		r.appendEntry(pb.Entry{Type: pb.EntryConfChange})
		r.becomeCandidate()
		r.becomeLeader()
	}()
}

// TestAddNode tests that addNode could update pendingConf and nodes correctly.
func TestAddNode(t *testing.T) {
	r := newRaft(1, []uint64{1}, 10, 1)
	r.pendingConf = true
	r.addNode(2)
	if r.pendingConf != false {
		t.Errorf("pendingConf = %v, want false", r.pendingConf)
	}
	nodes := r.nodes()
	sort.Sort(uint64Slice(nodes))
	wnodes := []uint64{1, 2}
	if !reflect.DeepEqual(nodes, wnodes) {
		t.Errorf("nodes = %v, want %v", nodes, wnodes)
	}
}

// TestRemoveNode tests that removeNode could update pendingConf, nodes and
// and removed list correctly.
func TestRemoveNode(t *testing.T) {
	r := newRaft(1, []uint64{1, 2}, 10, 1)
	r.pendingConf = true
	r.removeNode(2)
	if r.pendingConf != false {
		t.Errorf("pendingConf = %v, want false", r.pendingConf)
	}
	w := []uint64{1}
	if g := r.nodes(); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}
	wremoved := map[uint64]bool{2: true}
	if !reflect.DeepEqual(r.removed, wremoved) {
		t.Errorf("rmNodes = %v, want %v", r.removed, wremoved)
	}
}

// TestRecvMsgDenied tests that state machine sets the removed list when
// handling msgDenied, and does not pass it to the actual stepX function.
func TestRecvMsgDenied(t *testing.T) {
	called := false
	fakeStep := func(r *raft, m pb.Message) {
		called = true
	}
	r := newRaft(1, []uint64{1, 2}, 10, 1)
	r.step = fakeStep
	r.Step(pb.Message{From: 2, Type: msgDenied})
	if called != false {
		t.Errorf("stepFunc called = %v , want %v", called, false)
	}
	wremoved := map[uint64]bool{1: true}
	if !reflect.DeepEqual(r.removed, wremoved) {
		t.Errorf("rmNodes = %v, want %v", r.removed, wremoved)
	}
}

// TestRecvMsgFromRemovedNode tests that state machine sends correct
// messages out when handling message from removed node, and does not
// pass it to the actual stepX function.
func TestRecvMsgFromRemovedNode(t *testing.T) {
	tests := []struct {
		from    uint64
		wmsgNum int
	}{
		{1, 0},
		{2, 1},
	}
	for i, tt := range tests {
		called := false
		fakeStep := func(r *raft, m pb.Message) {
			called = true
		}
		r := newRaft(1, []uint64{1}, 10, 1)
		r.step = fakeStep
		r.removeNode(tt.from)
		r.Step(pb.Message{From: tt.from, Type: msgVote})
		if called != false {
			t.Errorf("#%d: stepFunc called = %v , want %v", i, called, false)
		}
		if len(r.msgs) != tt.wmsgNum {
			t.Errorf("#%d: len(msgs) = %d, want %d", i, len(r.msgs), tt.wmsgNum)
		}
		for j, msg := range r.msgs {
			if msg.Type != msgDenied {
				t.Errorf("#%d.%d: msgType = %d, want %d", i, j, msg.Type, msgDenied)
			}
		}
	}
}

func TestPromotable(t *testing.T) {
	id := uint64(1)
	tests := []struct {
		peers []uint64
		wp    bool
	}{
		{[]uint64{1}, true},
		{[]uint64{1, 2, 3}, true},
		{[]uint64{}, false},
		{[]uint64{2, 3}, false},
	}
	for i, tt := range tests {
		r := &raft{id: id, prs: make(map[uint64]*progress)}
		for _, id := range tt.peers {
			r.prs[id] = &progress{}
		}
		if g := r.promotable(); g != tt.wp {
			t.Errorf("#%d: promotable = %v, want %v", i, g, tt.wp)
		}
	}
}

func ents(terms ...uint64) *raft {
	ents := []pb.Entry{{}}
	for _, term := range terms {
		ents = append(ents, pb.Entry{Term: term})
	}

	sm := &raft{raftLog: &raftLog{ents: ents}}
	sm.reset(0)
	return sm
}

type network struct {
	peers   map[uint64]Interface
	dropm   map[connem]float64
	ignorem map[uint64]bool
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [0, n).
func newNetwork(peers ...Interface) *network {
	size := len(peers)
	peerAddrs := make([]uint64, size)
	for i := 0; i < size; i++ {
		peerAddrs[i] = 1 + uint64(i)
	}

	npeers := make(map[uint64]Interface, size)

	for i, p := range peers {
		id := peerAddrs[i]
		switch v := p.(type) {
		case nil:
			sm := newRaft(id, peerAddrs, 10, 1)
			npeers[id] = sm
		case *raft:
			v.id = id
			v.prs = make(map[uint64]*progress)
			for i := 0; i < size; i++ {
				v.prs[peerAddrs[i]] = &progress{}
			}
			v.reset(0)
			npeers[id] = v
		case *blackHole:
			npeers[id] = v
		default:
			panic(fmt.Sprintf("unexpected state machine type: %T", p))
		}
	}
	return &network{
		peers:   npeers,
		dropm:   make(map[connem]float64),
		ignorem: make(map[uint64]bool),
	}
}

func (nw *network) send(msgs ...pb.Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		p.Step(m)
		msgs = append(msgs[1:], nw.filter(p.ReadMessages())...)
	}
}

func (nw *network) drop(from, to uint64, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(one, other uint64) {
	nw.drop(one, other, 1)
	nw.drop(other, one, 1)
}

func (nw *network) isolate(id uint64) {
	for i := 0; i < len(nw.peers); i++ {
		nid := uint64(i) + 1
		if nid != id {
			nw.drop(id, nid, 1.0)
			nw.drop(nid, id, 1.0)
		}
	}
}

func (nw *network) ignore(t uint64) {
	nw.ignorem[t] = true
}

func (nw *network) recover() {
	nw.dropm = make(map[connem]float64)
	nw.ignorem = make(map[uint64]bool)
}

func (nw *network) filter(msgs []pb.Message) []pb.Message {
	mm := []pb.Message{}
	for _, m := range msgs {
		if nw.ignorem[m.Type] {
			continue
		}
		switch m.Type {
		case msgHup:
			// hups never go over the network, so don't drop them but panic
			panic("unexpected msgHup")
		default:
			perc := nw.dropm[connem{m.From, m.To}]
			if n := rand.Float64(); n < perc {
				continue
			}
		}
		mm = append(mm, m)
	}
	return mm
}

type connem struct {
	from, to uint64
}

type blackHole struct{}

func (blackHole) Step(pb.Message) error      { return nil }
func (blackHole) ReadMessages() []pb.Message { return nil }

var nopStepper = &blackHole{}
