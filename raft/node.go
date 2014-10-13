package raft

import (
	"errors"
	"log"
	"reflect"

	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/third_party/code.google.com/p/go.net/context"
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead       uint64
	RaftState  StateType
	Nodes      []uint64
	ShouldStop bool
}

func (a *SoftState) equal(b *SoftState) bool {
	return reflect.DeepEqual(a, b)
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	Messages []pb.Message
}

type compact struct {
	index uint64
	nodes []uint64
	data  []byte
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Index == 0
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) || !IsEmptySnap(rd.Snapshot) ||
		len(rd.Entries) > 0 || len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0
}

// Node represents a node in a raft cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	Tick()
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log.
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes config change.
	// At most one ConfChange can be in the process of going through consensus.
	// Application needs to call ApplyConfChange when applying EntryConfChange type entry.
	ProposeConfChange(ctx context.Context, cc pb.ConfChange) error
	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	Step(ctx context.Context, msg pb.Message) error
	// Ready returns a channel that returns the current point-in-time state
	Ready() <-chan Ready
	// ApplyConfChange applies config change to the local node.
	// TODO: reject existing node when add node
	// TODO: reject non-existant node when remove node
	ApplyConfChange(cc pb.ConfChange)
	// Stop performs any necessary termination of the Node
	Stop()
	// Compact discards the entrire log up to the given index. It also
	// generates a raft snapshot containing the given nodes configuration
	// and the given snapshot data.
	// It is the caller's responsibility to ensure the given configuration
	// and snapshot data match the actual point-in-time configuration and snapshot
	// at the given index.
	Compact(index uint64, nodes []uint64, d []byte)
}

type Peer struct {
	ID      uint64
	Context []byte
}

// StartNode returns a new Node given a unique raft id, a list of raft peers, and
// the election and heartbeat timeouts in units of ticks.
// It also builds ConfChangeAddNode entry for each peer and puts them at the head of the log.
func StartNode(id uint64, peers []Peer, election, heartbeat int) Node {
	n := newNode()
	r := newRaft(id, nil, election, heartbeat)

	ents := make([]pb.Entry, len(peers))
	for i, peer := range peers {
		cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
		data, err := cc.Marshal()
		if err != nil {
			panic("unexpected marshal error")
		}
		ents[i] = pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data}
	}
	r.raftLog.append(0, ents...)
	r.raftLog.committed = uint64(len(ents))

	go n.run(r)
	return &n
}

// RestartNode is identical to StartNode but takes an initial State and a slice
// of entries. Generally this is used when restarting from a stable storage
// log.
func RestartNode(id uint64, election, heartbeat int, snapshot *pb.Snapshot, st pb.HardState, ents []pb.Entry) Node {
	n := newNode()
	r := newRaft(id, nil, election, heartbeat)
	if snapshot != nil {
		r.restore(*snapshot)
	}
	r.loadState(st)
	r.loadEnts(ents)
	go n.run(r)
	return &n
}

// node is the canonical implementation of the Node interface
type node struct {
	propc    chan pb.Message
	recvc    chan pb.Message
	compactc chan compact
	confc    chan pb.ConfChange
	readyc   chan Ready
	tickc    chan struct{}
	done     chan struct{}
}

func newNode() node {
	return node{
		propc:    make(chan pb.Message),
		recvc:    make(chan pb.Message),
		compactc: make(chan compact),
		confc:    make(chan pb.ConfChange),
		readyc:   make(chan Ready),
		tickc:    make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (n *node) Stop() {
	close(n.done)
}

func (n *node) run(r *raft) {
	var propc chan pb.Message
	var readyc chan Ready

	lead := None
	prevSoftSt := r.softState()
	prevHardSt := r.HardState
	prevSnapi := r.raftLog.snapshot.Index

	for {
		rd := newReady(r, prevSoftSt, prevHardSt, prevSnapi)
		if rd.containsUpdates() {
			readyc = n.readyc
		} else {
			readyc = nil
		}

		if rd.SoftState != nil && lead != rd.SoftState.Lead {
			log.Printf("raft: leader changed from %#x to %#x", lead, rd.SoftState.Lead)
			lead = rd.SoftState.Lead
			if r.hasLeader() {
				propc = n.propc
			} else {
				propc = nil
			}
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		case m := <-propc:
			m.From = r.id
			r.Step(m)
		case m := <-n.recvc:
			r.Step(m) // raft never returns an error
		case c := <-n.compactc:
			r.compact(c.index, c.nodes, c.data)
		case cc := <-n.confc:
			switch cc.Type {
			case pb.ConfChangeAddNode:
				r.addNode(cc.NodeID)
			case pb.ConfChangeRemoveNode:
				r.removeNode(cc.NodeID)
			default:
				panic("unexpected conf type")
			}
		case <-n.tickc:
			r.tick()
		case readyc <- rd:
			if rd.SoftState != nil {
				prevSoftSt = rd.SoftState
			}
			if !IsEmptyHardState(rd.HardState) {
				prevHardSt = rd.HardState
			}
			if !IsEmptySnap(rd.Snapshot) {
				prevSnapi = rd.Snapshot.Index
			}
			// TODO(yichengq): we assume that all committed config
			// entries will be applied to make things easy for now.
			// TODO(yichengq): it may have race because applied is set
			// before entries are applied.
			r.raftLog.resetNextEnts()
			r.raftLog.resetUnstable()
			r.msgs = nil
		case <-n.done:
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) Campaign(ctx context.Context) error {
	return n.step(ctx, pb.Message{Type: msgHup})
}

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.step(ctx, pb.Message{Type: msgProp, Entries: []pb.Entry{{Data: data}}})
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if m.Type == msgHup || m.Type == msgBeat {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return n.Step(ctx, pb.Message{Type: msgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}}})
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *node) step(ctx context.Context, m pb.Message) error {
	ch := n.recvc
	if m.Type == msgProp {
		ch = n.propc
	}

	select {
	case ch <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

func (n *node) Ready() <-chan Ready {
	return n.readyc
}

func (n *node) ApplyConfChange(cc pb.ConfChange) {
	select {
	case n.confc <- cc:
	case <-n.done:
	}
}

func (n *node) Compact(index uint64, nodes []uint64, d []byte) {
	select {
	case n.compactc <- compact{index, nodes, d}:
	case <-n.done:
	}
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState, prevSnapi uint64) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEnts(),
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if !isHardStateEqual(r.HardState, prevHardSt) {
		rd.HardState = r.HardState
	}
	if prevSnapi != r.raftLog.snapshot.Index {
		rd.Snapshot = r.raftLog.snapshot
	}
	return rd
}
