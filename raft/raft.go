package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"

	pb "github.com/coreos/etcd/raft/raftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

type messageType uint64

const (
	msgHup uint64 = iota
	msgBeat
	msgProp
	msgApp
	msgAppResp
	msgVote
	msgVoteResp
	msgSnap
	msgDenied
)

var mtmap = [...]string{
	"msgHup",
	"msgBeat",
	"msgProp",
	"msgApp",
	"msgAppResp",
	"msgVote",
	"msgVoteResp",
	"msgSnap",
	"msgDenied",
}

func (mt messageType) String() string {
	return mtmap[uint64(mt)]
}

var errNoLeader = errors.New("no leader")

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

type progress struct {
	match, next uint64
}

func (pr *progress) update(n uint64) {
	pr.match = n
	pr.next = n + 1
}

// maybeDecrTo returns false if the given to index comes from an out of order message.
// Otherwise it decreases the progress next index and returns true.
func (pr *progress) maybeDecrTo(to uint64) bool {
	// the rejection must be stale if the
	// progress has matched with follower
	// or "to" does not match next - 1
	if pr.match != 0 || pr.next-1 != to {
		return false
	}

	if pr.next--; pr.next < 1 {
		pr.next = 1
	}
	return true
}

func (pr *progress) String() string {
	return fmt.Sprintf("n=%d m=%d", pr.next, pr.match)
}

// uint64Slice implements sort interface
type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type raft struct {
	pb.HardState

	id uint64

	// the log
	raftLog *raftLog

	prs map[uint64]*progress

	state StateType

	votes map[uint64]bool

	msgs []pb.Message

	// the leader id
	lead uint64

	// New configuration is ignored if there exists unapplied configuration.
	pendingConf bool

	// TODO: need GC and recovery from snapshot
	removed map[uint64]bool

	elapsed          int // number of ticks since the last msg
	heartbeatTimeout int
	electionTimeout  int
	tick             func()
	step             stepFunc
}

func newRaft(id uint64, peers []uint64, election, heartbeat int) *raft {
	if id == None {
		panic("cannot use none id")
	}
	rand.Seed(int64(id))
	r := &raft{
		id:               id,
		lead:             None,
		raftLog:          newLog(),
		prs:              make(map[uint64]*progress),
		removed:          make(map[uint64]bool),
		electionTimeout:  election,
		heartbeatTimeout: heartbeat,
	}
	for _, p := range peers {
		r.prs[p] = &progress{}
	}
	r.becomeFollower(0, None)
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) shouldStop() bool { return r.removed[r.id] }

func (r *raft) softState() *SoftState {
	return &SoftState{Lead: r.lead, RaftState: r.state, Nodes: r.nodes(), ShouldStop: r.shouldStop()}
}

func (r *raft) String() string {
	s := fmt.Sprintf(`state=%v term=%d`, r.state, r.Term)
	switch r.state {
	case StateFollower:
		s += fmt.Sprintf(" vote=%v lead=%v", r.Vote, r.lead)
	case StateCandidate:
		s += fmt.Sprintf(` votes="%v"`, r.votes)
	case StateLeader:
		s += fmt.Sprintf(` prs="%v"`, r.prs)
	}
	return s
}

func (r *raft) poll(id uint64, v bool) (granted int) {
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

// send persists state to stable storage and then sends to its mailbox.
func (r *raft) send(m pb.Message) {
	m.From = r.id
	// do not attach term to msgProp
	// proposals are a way to forward to the leader and
	// should be treated as local message.
	if m.Type != msgProp {
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

// sendAppend sends RRPC, with entries to the given peer.
func (r *raft) sendAppend(to uint64) {
	pr := r.prs[to]
	m := pb.Message{}
	m.To = to
	m.Index = pr.next - 1
	if r.needSnapshot(m.Index) {
		m.Type = msgSnap
		m.Snapshot = r.raftLog.snapshot
	} else {
		m.Type = msgApp
		m.LogTerm = r.raftLog.term(pr.next - 1)
		m.Entries = r.raftLog.entries(pr.next)
		m.Commit = r.raftLog.committed
	}
	r.send(m)
}

// sendHeartbeat sends an empty msgApp
func (r *raft) sendHeartbeat(to uint64) {
	m := pb.Message{
		To:   to,
		Type: msgApp,
	}
	r.send(m)
}

// bcastAppend sends RRPC, with entries to all peers that are not up-to-date according to r.mis.
func (r *raft) bcastAppend() {
	for i := range r.prs {
		if i == r.id {
			continue
		}
		r.sendAppend(i)
	}
}

// bcastHeartbeat sends RRPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	for i := range r.prs {
		if i == r.id {
			continue
		}
		r.sendHeartbeat(i)
	}
}

func (r *raft) maybeCommit() bool {
	// TODO(bmizerany): optimize.. Currently naive
	mis := make(uint64Slice, 0, len(r.prs))
	for i := range r.prs {
		mis = append(mis, r.prs[i].match)
	}
	sort.Sort(sort.Reverse(mis))
	mci := mis[r.q()-1]

	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) reset(term uint64) {
	r.Term = term
	r.lead = None
	r.Vote = None
	r.elapsed = 0
	r.votes = make(map[uint64]bool)
	for i := range r.prs {
		r.prs[i] = &progress{next: r.raftLog.lastIndex() + 1}
		if i == r.id {
			r.prs[i].match = r.raftLog.lastIndex()
		}
	}
	r.pendingConf = false
}

func (r *raft) q() int {
	return len(r.prs)/2 + 1
}

func (r *raft) appendEntry(e pb.Entry) {
	e.Term = r.Term
	e.Index = r.raftLog.lastIndex() + 1
	r.raftLog.append(r.raftLog.lastIndex(), e)
	r.prs[r.id].update(r.raftLog.lastIndex())
	r.maybeCommit()
}

// tickElection is ran by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	if !r.promotable() {
		r.elapsed = 0
		return
	}
	r.elapsed++
	if r.isElectionTimeout() {
		r.elapsed = 0
		r.Step(pb.Message{From: r.id, Type: msgHup})
	}
}

// tickHeartbeat is ran by leaders to send a msgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	r.elapsed++
	if r.elapsed > r.heartbeatTimeout {
		r.elapsed = 0
		r.Step(pb.Message{From: r.id, Type: msgBeat})
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
}

func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.state = StateCandidate
}

func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	for _, e := range r.raftLog.entries(r.raftLog.committed + 1) {
		if e.Type != pb.EntryConfChange {
			continue
		}
		if r.pendingConf {
			panic("unexpected double uncommitted config entry")
		}
		r.pendingConf = true
	}
	r.appendEntry(pb.Entry{Data: nil})
}

func (r *raft) ReadMessages() []pb.Message {
	msgs := r.msgs
	r.msgs = make([]pb.Message, 0)

	return msgs
}

func (r *raft) campaign() {
	r.becomeCandidate()
	if r.q() == r.poll(r.id, true) {
		r.becomeLeader()
	}
	for i := range r.prs {
		if i == r.id {
			continue
		}
		lasti := r.raftLog.lastIndex()
		r.send(pb.Message{To: i, Type: msgVote, Index: lasti, LogTerm: r.raftLog.term(lasti)})
	}
}

func (r *raft) Step(m pb.Message) error {
	// TODO(bmizerany): this likely allocs - prevent that.
	defer func() { r.Commit = r.raftLog.committed }()

	if r.removed[m.From] {
		if m.From != r.id {
			r.send(pb.Message{To: m.From, Type: msgDenied})
		}
		// TODO: return an error?
		return nil
	}
	if m.Type == msgDenied {
		r.removed[r.id] = true
		// TODO: return an error?
		return nil
	}

	if m.Type == msgHup {
		r.campaign()
	}

	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		lead := m.From
		if m.Type == msgVote {
			lead = None
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		// ignore
		return nil
	}
	r.step(r, m)
	return nil
}

func (r *raft) handleAppendEntries(m pb.Message) {
	if r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...) {
		r.send(pb.Message{To: m.From, Type: msgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.send(pb.Message{To: m.From, Type: msgAppResp, Index: m.Index, Reject: true})
	}
}

func (r *raft) handleSnapshot(m pb.Message) {
	if r.restore(m.Snapshot) {
		r.send(pb.Message{To: m.From, Type: msgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.send(pb.Message{To: m.From, Type: msgAppResp, Index: r.raftLog.committed})
	}
}

func (r *raft) addNode(id uint64) {
	r.setProgress(id, 0, r.raftLog.lastIndex()+1)
	r.pendingConf = false
}

func (r *raft) removeNode(id uint64) {
	r.delProgress(id)
	r.pendingConf = false
	r.removed[id] = true
}

type stepFunc func(r *raft, m pb.Message)

func stepLeader(r *raft, m pb.Message) {
	switch m.Type {
	case msgBeat:
		r.bcastHeartbeat()
	case msgProp:
		if len(m.Entries) != 1 {
			panic("unexpected length(entries) of a msgProp")
		}
		e := m.Entries[0]
		if e.Type == pb.EntryConfChange {
			if r.pendingConf {
				return
			}
			r.pendingConf = true
		}
		r.appendEntry(e)
		r.bcastAppend()
	case msgAppResp:
		if m.Reject {
			if r.prs[m.From].maybeDecrTo(m.Index) {
				r.sendAppend(m.From)
			}
		} else {
			r.prs[m.From].update(m.Index)
			if r.maybeCommit() {
				r.bcastAppend()
			}
		}
	case msgVote:
		r.send(pb.Message{To: m.From, Type: msgVoteResp, Reject: true})
	}
}

func stepCandidate(r *raft, m pb.Message) {
	switch m.Type {
	case msgProp:
		panic("no leader")
	case msgApp:
		r.becomeFollower(r.Term, m.From)
		r.handleAppendEntries(m)
	case msgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case msgVote:
		r.send(pb.Message{To: m.From, Type: msgVoteResp, Reject: true})
	case msgVoteResp:
		gr := r.poll(m.From, !m.Reject)
		switch r.q() {
		case gr:
			r.becomeLeader()
			r.bcastAppend()
		case len(r.votes) - gr:
			r.becomeFollower(r.Term, None)
		}
	}
}

func stepFollower(r *raft, m pb.Message) {
	switch m.Type {
	case msgProp:
		if r.lead == None {
			panic("no leader")
		}
		m.To = r.lead
		r.send(m)
	case msgApp:
		r.elapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case msgSnap:
		r.elapsed = 0
		r.handleSnapshot(m)
	case msgVote:
		if (r.Vote == None || r.Vote == m.From) && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			r.elapsed = 0
			r.Vote = m.From
			r.send(pb.Message{To: m.From, Type: msgVoteResp})
		} else {
			r.send(pb.Message{To: m.From, Type: msgVoteResp, Reject: true})
		}
	}
}

func (r *raft) compact(index uint64, nodes []uint64, d []byte) {
	if index > r.raftLog.applied {
		panic(fmt.Sprintf("raft: compact index (%d) exceeds applied index (%d)", index, r.raftLog.applied))
	}
	// We do not get the removed nodes at the given index.
	// We get the removed nodes at current index. So a state machine might
	// have a newer verison of removed nodes after recovery. It is OK.
	r.raftLog.snap(d, index, r.raftLog.term(index), nodes, r.removedNodes())
	r.raftLog.compact(index)
}

// restore recovers the statemachine from a snapshot. It restores the log and the
// configuration of statemachine.
func (r *raft) restore(s pb.Snapshot) bool {
	if s.Index <= r.raftLog.committed {
		return false
	}

	r.raftLog.restore(s)
	r.prs = make(map[uint64]*progress)
	for _, n := range s.Nodes {
		if n == r.id {
			r.setProgress(n, r.raftLog.lastIndex(), r.raftLog.lastIndex()+1)
		} else {
			r.setProgress(n, 0, r.raftLog.lastIndex()+1)
		}
	}
	r.removed = make(map[uint64]bool)
	for _, n := range s.RemovedNodes {
		r.removed[n] = true
	}
	return true
}

func (r *raft) needSnapshot(i uint64) bool {
	if i < r.raftLog.offset {
		if r.raftLog.snapshot.Term == 0 {
			panic("need non-empty snapshot")
		}
		return true
	}
	return false
}

func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.prs))
	for k := range r.prs {
		nodes = append(nodes, k)
	}
	return nodes
}

func (r *raft) removedNodes() []uint64 {
	removed := make([]uint64, 0, len(r.removed))
	for k := range r.removed {
		removed = append(removed, k)
	}
	return removed
}

func (r *raft) setProgress(id, match, next uint64) {
	r.prs[id] = &progress{next: next, match: match}
}

func (r *raft) delProgress(id uint64) {
	delete(r.prs, id)
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	_, ok := r.prs[r.id]
	return ok
}

func (r *raft) loadEnts(ents []pb.Entry) {
	r.raftLog.load(ents)
}

func (r *raft) loadState(state pb.HardState) {
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
	r.Commit = state.Commit
}

// isElectionTimeout returns true if r.elapsed is greater than the
// randomized election timeout in (electiontimeout, 2 * electiontimeout - 1).
// Otherwise, it returns false.
func (r *raft) isElectionTimeout() bool {
	d := r.elapsed - r.electionTimeout
	if d < 0 {
		return false
	}
	return d > rand.Int()%r.electionTimeout
}
