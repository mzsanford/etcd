package etcdserver

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/store"
	"github.com/coreos/etcd/third_party/code.google.com/p/go.net/context"
)

func TestGetExpirationTime(t *testing.T) {
	tests := []struct {
		r    pb.Request
		want time.Time
	}{
		{
			pb.Request{Expiration: 0},
			time.Time{},
		},
		{
			pb.Request{Expiration: 60000},
			time.Unix(0, 60000),
		},
		{
			pb.Request{Expiration: -60000},
			time.Unix(0, -60000),
		},
	}

	for i, tt := range tests {
		got := getExpirationTime(&tt.r)
		if !reflect.DeepEqual(tt.want, got) {
			t.Errorf("#%d: incorrect expiration time: want=%v got=%v", i, tt.want, got)
		}
	}
}

// TestDoLocalAction tests requests which do not need to go through raft to be applied,
// and are served through local data.
func TestDoLocalAction(t *testing.T) {
	tests := []struct {
		req pb.Request

		wresp    Response
		werr     error
		wactions []action
	}{
		{
			pb.Request{Method: "GET", ID: 1, Wait: true},
			Response{Watcher: &stubWatcher{}}, nil, []action{action{name: "Watch"}},
		},
		{
			pb.Request{Method: "GET", ID: 1},
			Response{Event: &store.Event{}}, nil,
			[]action{
				action{
					name:   "Get",
					params: []interface{}{"", false, false},
				},
			},
		},
		{
			pb.Request{Method: "BADMETHOD", ID: 1},
			Response{}, ErrUnknownMethod, []action{},
		},
	}
	for i, tt := range tests {
		st := &storeRecorder{}
		srv := &EtcdServer{Store: st}
		resp, err := srv.Do(context.TODO(), tt.req)

		if err != tt.werr {
			t.Fatalf("#%d: err = %+v, want %+v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(resp, tt.wresp) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, tt.wresp)
		}
		gaction := st.Action()
		if !reflect.DeepEqual(gaction, tt.wactions) {
			t.Errorf("#%d: action = %+v, want %+v", i, gaction, tt.wactions)
		}
	}
}

// TestDoBadLocalAction tests server requests which do not need to go through consensus,
// and return errors when they fetch from local data.
func TestDoBadLocalAction(t *testing.T) {
	storeErr := fmt.Errorf("bah")
	tests := []struct {
		req pb.Request

		wactions []action
	}{
		{
			pb.Request{Method: "GET", ID: 1, Wait: true},
			[]action{action{name: "Watch"}},
		},
		{
			pb.Request{Method: "GET", ID: 1},
			[]action{action{name: "Get"}},
		},
	}
	for i, tt := range tests {
		st := &errStoreRecorder{err: storeErr}
		srv := &EtcdServer{Store: st}
		resp, err := srv.Do(context.Background(), tt.req)

		if err != storeErr {
			t.Fatalf("#%d: err = %+v, want %+v", i, err, storeErr)
		}
		if !reflect.DeepEqual(resp, Response{}) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, Response{})
		}
		gaction := st.Action()
		if !reflect.DeepEqual(gaction, tt.wactions) {
			t.Errorf("#%d: action = %+v, want %+v", i, gaction, tt.wactions)
		}
	}
}

func TestApply(t *testing.T) {
	tests := []struct {
		req pb.Request

		wresp    Response
		wactions []action
	}{
		// POST ==> Create
		{
			pb.Request{Method: "POST", ID: 1},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Create",
					params: []interface{}{"", false, "", true, time.Time{}},
				},
			},
		},
		// POST ==> Create, with expiration
		{
			pb.Request{Method: "POST", ID: 1, Expiration: 1337},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Create",
					params: []interface{}{"", false, "", true, time.Unix(0, 1337)},
				},
			},
		},
		// POST ==> Create, with dir
		{
			pb.Request{Method: "POST", ID: 1, Dir: true},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Create",
					params: []interface{}{"", true, "", true, time.Time{}},
				},
			},
		},
		// PUT ==> Set
		{
			pb.Request{Method: "PUT", ID: 1},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Set",
					params: []interface{}{"", false, "", time.Time{}},
				},
			},
		},
		// PUT ==> Set, with dir
		{
			pb.Request{Method: "PUT", ID: 1, Dir: true},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Set",
					params: []interface{}{"", true, "", time.Time{}},
				},
			},
		},
		// PUT with PrevExist=true ==> Update
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: boolp(true)},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Update",
					params: []interface{}{"", "", time.Time{}},
				},
			},
		},
		// PUT with PrevExist=false ==> Create
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: boolp(false)},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Create",
					params: []interface{}{"", false, "", false, time.Time{}},
				},
			},
		},
		// PUT with PrevExist=true *and* PrevIndex set ==> Update
		// TODO(jonboulle): is this expected?!
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: boolp(true), PrevIndex: 1},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Update",
					params: []interface{}{"", "", time.Time{}},
				},
			},
		},
		// PUT with PrevExist=false *and* PrevIndex set ==> Create
		// TODO(jonboulle): is this expected?!
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: boolp(false), PrevIndex: 1},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Create",
					params: []interface{}{"", false, "", false, time.Time{}},
				},
			},
		},
		// PUT with PrevIndex set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevIndex: 1},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "CompareAndSwap",
					params: []interface{}{"", "", uint64(1), "", time.Time{}},
				},
			},
		},
		// PUT with PrevValue set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevValue: "bar"},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "CompareAndSwap",
					params: []interface{}{"", "bar", uint64(0), "", time.Time{}},
				},
			},
		},
		// PUT with PrevIndex and PrevValue set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevIndex: 1, PrevValue: "bar"},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "CompareAndSwap",
					params: []interface{}{"", "bar", uint64(1), "", time.Time{}},
				},
			},
		},
		// DELETE ==> Delete
		{
			pb.Request{Method: "DELETE", ID: 1},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Delete",
					params: []interface{}{"", false, false},
				},
			},
		},
		// DELETE with PrevIndex set ==> CompareAndDelete
		{
			pb.Request{Method: "DELETE", ID: 1, PrevIndex: 1},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "CompareAndDelete",
					params: []interface{}{"", "", uint64(1)},
				},
			},
		},
		// DELETE with PrevValue set ==> CompareAndDelete
		{
			pb.Request{Method: "DELETE", ID: 1, PrevValue: "bar"},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "CompareAndDelete",
					params: []interface{}{"", "bar", uint64(0)},
				},
			},
		},
		// DELETE with PrevIndex *and* PrevValue set ==> CompareAndDelete
		{
			pb.Request{Method: "DELETE", ID: 1, PrevIndex: 5, PrevValue: "bar"},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "CompareAndDelete",
					params: []interface{}{"", "bar", uint64(5)},
				},
			},
		},
		// QGET ==> Get
		{
			pb.Request{Method: "QGET", ID: 1},
			Response{Event: &store.Event{}},
			[]action{
				action{
					name:   "Get",
					params: []interface{}{"", false, false},
				},
			},
		},
		// SYNC ==> DeleteExpiredKeys
		{
			pb.Request{Method: "SYNC", ID: 1},
			Response{},
			[]action{
				action{
					name:   "DeleteExpiredKeys",
					params: []interface{}{time.Unix(0, 0)},
				},
			},
		},
		{
			pb.Request{Method: "SYNC", ID: 1, Time: 12345},
			Response{},
			[]action{
				action{
					name:   "DeleteExpiredKeys",
					params: []interface{}{time.Unix(0, 12345)},
				},
			},
		},
		// Unknown method - error
		{
			pb.Request{Method: "BADMETHOD", ID: 1},
			Response{err: ErrUnknownMethod},
			[]action{},
		},
	}

	for i, tt := range tests {
		st := &storeRecorder{}
		srv := &EtcdServer{Store: st}
		resp := srv.apply(tt.req)

		if !reflect.DeepEqual(resp, tt.wresp) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, tt.wresp)
		}
		gaction := st.Action()
		if !reflect.DeepEqual(gaction, tt.wactions) {
			t.Errorf("#%d: action = %#v, want %#v", i, gaction, tt.wactions)
		}
	}
}

func TestClusterOf1(t *testing.T) { testServer(t, 1) }
func TestClusterOf3(t *testing.T) { testServer(t, 3) }

func testServer(t *testing.T, ns int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ss := make([]*EtcdServer, ns)

	send := func(msgs []raftpb.Message) {
		for _, m := range msgs {
			t.Logf("m = %+v\n", m)
			ss[m.To-1].Node.Step(ctx, m)
		}
	}

	members := make([]int64, ns)
	for i := int64(0); i < ns; i++ {
		members[i] = i + 1
	}

	for i := int64(0); i < ns; i++ {
		id := i + 1
		n := raft.StartNode(id, members, 10, 1)
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		srv := &EtcdServer{
			Node:    n,
			Store:   store.New(),
			Send:    send,
			Storage: &storageRecorder{},
			Ticker:  tk.C,
		}
		srv.start()
		// TODO(xiangli): randomize election timeout
		// then remove this sleep.
		time.Sleep(1 * time.Millisecond)
		ss[i] = srv
	}

	for i := 1; i <= 10; i++ {
		r := pb.Request{
			Method: "PUT",
			ID:     int64(i),
			Path:   "/foo",
			Val:    "bar",
		}
		j := rand.Intn(len(ss))
		t.Logf("ss = %d", j)
		resp, err := ss[j].Do(ctx, r)
		if err != nil {
			t.Fatal(err)
		}

		g, w := resp.Event.Node, &store.NodeExtern{
			Key:           "/foo",
			ModifiedIndex: uint64(i),
			CreatedIndex:  uint64(i),
			Value:         stringp("bar"),
		}

		if !reflect.DeepEqual(g, w) {
			t.Error("value:", *g.Value)
			t.Errorf("g = %+v, w %+v", g, w)
		}
	}

	time.Sleep(10 * time.Millisecond)

	var last interface{}
	for i, sv := range ss {
		sv.Stop()
		g, _ := sv.Store.Get("/", true, true)
		if last != nil && !reflect.DeepEqual(last, g) {
			t.Errorf("server %d: Root = %#v, want %#v", i, g, last)
		}
		last = g
	}
}

func TestDoProposal(t *testing.T) {
	tests := []pb.Request{
		pb.Request{Method: "POST", ID: 1},
		pb.Request{Method: "PUT", ID: 1},
		pb.Request{Method: "DELETE", ID: 1},
		pb.Request{Method: "GET", ID: 1, Quorum: true},
	}

	for i, tt := range tests {
		ctx, _ := context.WithCancel(context.Background())
		n := raft.StartNode(0xBAD0, []int64{0xBAD0}, 10, 1)
		st := &storeRecorder{}
		tk := make(chan time.Time)
		// this makes <-tk always successful, which accelerates internal clock
		close(tk)
		srv := &EtcdServer{
			Node:    n,
			Store:   st,
			Send:    func(_ []raftpb.Message) {},
			Storage: &storageRecorder{},
			Ticker:  tk,
		}
		srv.start()
		resp, err := srv.Do(ctx, tt)
		srv.Stop()

		action := st.Action()
		if len(action) != 1 {
			t.Errorf("#%d: len(action) = %d, want 1", i, len(action))
		}
		if err != nil {
			t.Fatalf("#%d: err = %v, want nil", i, err)
		}
		wresp := Response{Event: &store.Event{}}
		if !reflect.DeepEqual(resp, wresp) {
			t.Errorf("#%d: resp = %v, want %v", i, resp, wresp)
		}
	}
}

func TestDoProposalCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// node cannot make any progress because there are two nodes
	n := raft.StartNode(0xBAD0, []int64{0xBAD0, 0xBAD1}, 10, 1)
	st := &storeRecorder{}
	wait := &waitRecorder{}
	srv := &EtcdServer{
		// TODO: use fake node for better testability
		Node:  n,
		Store: st,
		w:     wait,
	}

	done := make(chan struct{})
	var err error
	go func() {
		_, err = srv.Do(ctx, pb.Request{Method: "PUT", ID: 1})
		close(done)
	}()
	cancel()
	<-done

	gaction := st.Action()
	if len(gaction) != 0 {
		t.Errorf("len(action) = %v, want 0", len(gaction))
	}
	if err != context.Canceled {
		t.Fatalf("err = %v, want %v", err, context.Canceled)
	}
	w := []action{action{name: "Register1"}, action{name: "Trigger1"}}
	if !reflect.DeepEqual(wait.action, w) {
		t.Errorf("wait.action = %+v, want %+v", wait.action, w)
	}
}

func TestDoProposalStopped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// node cannot make any progress because there are two nodes
	n := raft.StartNode(0xBAD0, []int64{0xBAD0, 0xBAD1}, 10, 1)
	st := &storeRecorder{}
	tk := make(chan time.Time)
	// this makes <-tk always successful, which accelarates internal clock
	close(tk)
	srv := &EtcdServer{
		// TODO: use fake node for better testability
		Node:    n,
		Store:   st,
		Send:    func(_ []raftpb.Message) {},
		Storage: &storageRecorder{},
		Ticker:  tk,
	}
	srv.start()

	done := make(chan struct{})
	var err error
	go func() {
		_, err = srv.Do(ctx, pb.Request{Method: "PUT", ID: 1})
		close(done)
	}()
	srv.Stop()
	<-done

	action := st.Action()
	if len(action) != 0 {
		t.Errorf("len(action) = %v, want 0", len(action))
	}
	if err != ErrStopped {
		t.Errorf("err = %v, want %v", err, ErrStopped)
	}
}

// TestSync tests sync 1. is nonblocking 2. sends out SYNC request.
func TestSync(t *testing.T) {
	n := &nodeProposeDataRecorder{}
	srv := &EtcdServer{
		Node: n,
	}
	start := time.Now()
	srv.sync(defaultSyncTimeout)

	// check that sync is non-blocking
	if d := time.Since(start); d > time.Millisecond {
		t.Errorf("CallSyncTime = %v, want < %v", d, time.Millisecond)
	}

	pkg.ForceGosched()
	data := n.data()
	if len(data) != 1 {
		t.Fatalf("len(proposeData) = %d, want 1", len(data))
	}
	var r pb.Request
	if err := r.Unmarshal(data[0]); err != nil {
		t.Fatalf("unmarshal request error: %v", err)
	}
	if r.Method != "SYNC" {
		t.Errorf("method = %s, want SYNC", r.Method)
	}
}

// TestSyncTimeout tests the case that sync 1. is non-blocking 2. cancel request
// after timeout
func TestSyncTimeout(t *testing.T) {
	n := &nodeProposalBlockerRecorder{}
	srv := &EtcdServer{
		Node: n,
	}
	start := time.Now()
	srv.sync(0)

	// check that sync is non-blocking
	if d := time.Since(start); d > time.Millisecond {
		t.Errorf("CallSyncTime = %v, want < %v", d, time.Millisecond)
	}

	// give time for goroutine in sync to cancel
	// TODO: use fake clock
	pkg.ForceGosched()
	w := []action{action{name: "Propose blocked"}}
	if g := n.Action(); !reflect.DeepEqual(g, w) {
		t.Errorf("action = %v, want %v", g, w)
	}
}

// TODO: TestNoSyncWhenNoLeader

// blockingNodeProposer implements the node interface to allow users to
// block until Propose has been called and then verify the Proposed data
type blockingNodeProposer struct {
	ch chan []byte
	readyNode
}

func (n *blockingNodeProposer) Propose(_ context.Context, data []byte) error {
	n.ch <- data
	return nil
}

// TestSyncTrigger tests that the server proposes a SYNC request when its sync timer ticks
func TestSyncTrigger(t *testing.T) {
	n := &blockingNodeProposer{
		ch:        make(chan []byte),
		readyNode: *newReadyNode(),
	}
	st := make(chan time.Time, 1)
	srv := &EtcdServer{
		Node:       n,
		Store:      &storeRecorder{},
		Send:       func(_ []raftpb.Message) {},
		Storage:    &storageRecorder{},
		SyncTicker: st,
	}
	srv.start()
	// trigger the server to become a leader and accept sync requests
	n.readyc <- raft.Ready{
		SoftState: &raft.SoftState{
			RaftState: raft.StateLeader,
		},
	}
	// trigger a sync request
	st <- time.Time{}
	var data []byte
	select {
	case <-time.After(time.Second):
		t.Fatalf("did not receive proposed request as expected!")
	case data = <-n.ch:
	}
	srv.Stop()
	var req pb.Request
	if err := req.Unmarshal(data); err != nil {
		t.Fatalf("error unmarshalling data: %v", err)
	}
	if req.Method != "SYNC" {
		t.Fatalf("unexpected proposed request: %#v", req.Method)
	}
}

// snapshot should snapshot the store and cut the persistent
// TODO: node.Compact is called... we need to make the node an interface
func TestSnapshot(t *testing.T) {
	n := raft.StartNode(0xBAD0, []int64{0xBAD0}, 10, 1)
	defer n.Stop()
	st := &storeRecorder{}
	p := &storageRecorder{}
	s := &EtcdServer{
		Store:   st,
		Storage: p,
		Node:    n,
	}

	s.snapshot()
	gaction := st.Action()
	if len(gaction) != 1 {
		t.Fatalf("len(action) = %d, want 1", len(gaction))
	}
	if !reflect.DeepEqual(gaction[0], action{name: "Save"}) {
		t.Errorf("action = %s, want Save", gaction[0])
	}

	gaction = p.Action()
	if len(gaction) != 1 {
		t.Fatalf("len(action) = %d, want 1", len(gaction))
	}
	if !reflect.DeepEqual(gaction[0], action{name: "Cut"}) {
		t.Errorf("action = %s, want Cut", gaction[0])
	}
}

// Applied > SnapCount should trigger a SaveSnap event
func TestTriggerSnap(t *testing.T) {
	ctx := context.Background()
	n := raft.StartNode(0xBAD0, []int64{0xBAD0}, 10, 1)
	n.Campaign(ctx)
	st := &storeRecorder{}
	p := &storageRecorder{}
	s := &EtcdServer{
		Store:     st,
		Send:      func(_ []raftpb.Message) {},
		Storage:   p,
		Node:      n,
		SnapCount: 10,
	}

	s.start()
	for i := 0; int64(i) < s.SnapCount; i++ {
		s.Do(ctx, pb.Request{Method: "PUT", ID: 1})
	}
	time.Sleep(time.Millisecond)
	s.Stop()

	gaction := p.Action()
	// each operation is recorded as a Save
	// Nop + SnapCount * Puts + Cut + SaveSnap = Save + SnapCount * Save + Cut + SaveSnap
	if len(gaction) != 3+int(s.SnapCount) {
		t.Fatalf("len(action) = %d, want %d", len(gaction), 3+int(s.SnapCount))
	}
	if !reflect.DeepEqual(gaction[12], action{name: "SaveSnap"}) {
		t.Errorf("action = %s, want SaveSnap", gaction[12])
	}
}

// TestRecvSnapshot tests when it receives a snapshot from raft leader,
// it should trigger storage.SaveSnap and also store.Recover.
func TestRecvSnapshot(t *testing.T) {
	n := newReadyNode()
	st := &storeRecorder{}
	p := &storageRecorder{}
	s := &EtcdServer{
		Store:   st,
		Send:    func(_ []raftpb.Message) {},
		Storage: p,
		Node:    n,
	}

	s.start()
	n.readyc <- raft.Ready{Snapshot: raftpb.Snapshot{Index: 1}}
	// make goroutines move forward to receive snapshot
	pkg.ForceGosched()
	s.Stop()

	wactions := []action{action{name: "Recovery"}}
	if g := st.Action(); !reflect.DeepEqual(g, wactions) {
		t.Errorf("store action = %v, want %v", g, wactions)
	}
	wactions = []action{action{name: "Save"}, action{name: "SaveSnap"}}
	if g := p.Action(); !reflect.DeepEqual(g, wactions) {
		t.Errorf("storage action = %v, want %v", g, wactions)
	}
}

// TestRecvSlowSnapshot tests that slow snapshot will not be applied
// to store.
func TestRecvSlowSnapshot(t *testing.T) {
	n := newReadyNode()
	st := &storeRecorder{}
	s := &EtcdServer{
		Store:   st,
		Send:    func(_ []raftpb.Message) {},
		Storage: &storageRecorder{},
		Node:    n,
	}

	s.start()
	n.readyc <- raft.Ready{Snapshot: raftpb.Snapshot{Index: 1}}
	// make goroutines move forward to receive snapshot
	pkg.ForceGosched()
	action := st.Action()

	n.readyc <- raft.Ready{Snapshot: raftpb.Snapshot{Index: 1}}
	// make goroutines move forward to receive snapshot
	pkg.ForceGosched()
	s.Stop()

	if g := st.Action(); !reflect.DeepEqual(g, action) {
		t.Errorf("store action = %v, want %v", g, action)
	}
}

// TestAddNode tests AddNode can propose and perform node addition.
func TestAddNode(t *testing.T) {
	n := newNodeConfChangeCommitterRecorder()
	s := &EtcdServer{
		Node:    n,
		Store:   &storeRecorder{},
		Send:    func(_ []raftpb.Message) {},
		Storage: &storageRecorder{},
	}
	s.start()
	s.AddNode(context.TODO(), 1, []byte("foo"))
	gaction := n.Action()
	s.Stop()

	wactions := []action{action{name: "ProposeConfChange:ConfChangeAddNode"}, action{name: "ApplyConfChange:ConfChangeAddNode"}}
	if !reflect.DeepEqual(gaction, wactions) {
		t.Errorf("action = %v, want %v", gaction, wactions)
	}
}

// TestRemoveNode tests RemoveNode can propose and perform node removal.
func TestRemoveNode(t *testing.T) {
	n := newNodeConfChangeCommitterRecorder()
	s := &EtcdServer{
		Node:    n,
		Store:   &storeRecorder{},
		Send:    func(_ []raftpb.Message) {},
		Storage: &storageRecorder{},
	}
	s.start()
	s.RemoveNode(context.TODO(), 1)
	gaction := n.Action()
	s.Stop()

	wactions := []action{action{name: "ProposeConfChange:ConfChangeRemoveNode"}, action{name: "ApplyConfChange:ConfChangeRemoveNode"}}
	if !reflect.DeepEqual(gaction, wactions) {
		t.Errorf("action = %v, want %v", gaction, wactions)
	}
}

// TODO: test wait trigger correctness in multi-server case

func TestPublish(t *testing.T) {
	n := &nodeProposeDataRecorder{}
	cs := mustClusterStore(t, []Member{{ID: 1, Name: "node1"}})
	ch := make(chan interface{}, 1)
	// simulate that request has gone through consensus
	ch <- Response{}
	w := &waitWithResponse{ch: ch}
	srv := &EtcdServer{
		Name:         "node1",
		ClientURLs:   []url.URL{{Scheme: "http", Host: "a"}, {Scheme: "http", Host: "b"}},
		Node:         n,
		ClusterStore: cs,
		w:            w,
	}
	srv.publish(time.Hour)

	data := n.data()
	if len(data) != 1 {
		t.Fatalf("len(proposeData) = %d, want 1", len(data))
	}
	var r pb.Request
	if err := r.Unmarshal(data[0]); err != nil {
		t.Fatalf("unmarshal request error: %v", err)
	}
	if r.Method != "PUT" {
		t.Errorf("method = %s, want PUT", r.Method)
	}
	wm := Member{ID: 1, Name: "node1", ClientURLs: []string{"http://a", "http://b"}}
	if r.Path != wm.storeKey() {
		t.Errorf("path = %s, want %s", r.Path, wm.storeKey())
	}
	var gm Member
	if err := json.Unmarshal([]byte(r.Val), &gm); err != nil {
		t.Fatalf("unmarshal val error: %v", err)
	}
	if !reflect.DeepEqual(gm, wm) {
		t.Errorf("member = %v, want %v", gm, wm)
	}
}

// TestPublishStopped tests that publish will be stopped if server is stopped.
func TestPublishStopped(t *testing.T) {
	cs := mustClusterStore(t, []Member{{ID: 1, Name: "node1"}})
	srv := &EtcdServer{
		Name:         "node1",
		Node:         &nodeRecorder{},
		ClusterStore: cs,
		w:            &waitRecorder{},
		done:         make(chan struct{}),
	}
	srv.Stop()
	srv.publish(time.Hour)
}

// TestPublishRetry tests that publish will keep retry until success.
func TestPublishRetry(t *testing.T) {
	n := &nodeRecorder{}
	cs := mustClusterStore(t, []Member{{ID: 1, Name: "node1"}})
	srv := &EtcdServer{
		Name:         "node1",
		Node:         n,
		ClusterStore: cs,
		w:            &waitRecorder{},
		done:         make(chan struct{}),
	}
	time.AfterFunc(500*time.Microsecond, srv.Stop)
	srv.publish(10 * time.Nanosecond)

	action := n.Action()
	// multiple Propose + Stop
	if len(action) < 3 {
		t.Errorf("len(action) = %d, want >= 3", action)
	}
}

func TestGetBool(t *testing.T) {
	tests := []struct {
		b    *bool
		wb   bool
		wset bool
	}{
		{nil, false, false},
		{boolp(true), true, true},
		{boolp(false), false, true},
	}
	for i, tt := range tests {
		b, set := getBool(tt.b)
		if b != tt.wb {
			t.Errorf("#%d: value = %v, want %v", i, b, tt.wb)
		}
		if set != tt.wset {
			t.Errorf("#%d: set = %v, want %v", i, set, tt.wset)
		}
	}
}

func TestGenID(t *testing.T) {
	// Sanity check that the GenID function has been seeded appropriately
	// (math/rand is seeded with 1 by default)
	r := rand.NewSource(int64(1))
	var n int64
	for n == 0 {
		n = r.Int63()
	}
	if n == GenID() {
		t.Fatalf("GenID's rand seeded with 1!")
	}
}

type action struct {
	name   string
	params []interface{}
}

type recorder struct {
	sync.Mutex
	actions []action
}

func (r *recorder) record(a action) {
	r.Lock()
	r.actions = append(r.actions, a)
	r.Unlock()
}
func (r *recorder) Action() []action {
	r.Lock()
	cpy := make([]action, len(r.actions))
	copy(cpy, r.actions)
	r.Unlock()
	return cpy
}

type storeRecorder struct {
	recorder
}

func (s *storeRecorder) Version() int  { return 0 }
func (s *storeRecorder) Index() uint64 { return 0 }
func (s *storeRecorder) Get(path string, recursive, sorted bool) (*store.Event, error) {
	s.record(action{
		name:   "Get",
		params: []interface{}{path, recursive, sorted},
	})
	return &store.Event{}, nil
}
func (s *storeRecorder) Set(path string, dir bool, val string, expr time.Time) (*store.Event, error) {
	s.record(action{
		name:   "Set",
		params: []interface{}{path, dir, val, expr},
	})
	return &store.Event{}, nil
}
func (s *storeRecorder) Update(path, val string, expr time.Time) (*store.Event, error) {
	s.record(action{
		name:   "Update",
		params: []interface{}{path, val, expr},
	})
	return &store.Event{}, nil
}
func (s *storeRecorder) Create(path string, dir bool, val string, uniq bool, exp time.Time) (*store.Event, error) {
	s.record(action{
		name:   "Create",
		params: []interface{}{path, dir, val, uniq, exp},
	})
	return &store.Event{}, nil
}
func (s *storeRecorder) CompareAndSwap(path, prevVal string, prevIdx uint64, val string, expr time.Time) (*store.Event, error) {
	s.record(action{
		name:   "CompareAndSwap",
		params: []interface{}{path, prevVal, prevIdx, val, expr},
	})
	return &store.Event{}, nil
}
func (s *storeRecorder) Delete(path string, dir, recursive bool) (*store.Event, error) {
	s.record(action{
		name:   "Delete",
		params: []interface{}{path, dir, recursive},
	})
	return &store.Event{}, nil
}
func (s *storeRecorder) CompareAndDelete(path, prevVal string, prevIdx uint64) (*store.Event, error) {
	s.record(action{
		name:   "CompareAndDelete",
		params: []interface{}{path, prevVal, prevIdx},
	})
	return &store.Event{}, nil
}
func (s *storeRecorder) Watch(_ string, _, _ bool, _ uint64) (store.Watcher, error) {
	s.record(action{name: "Watch"})
	return &stubWatcher{}, nil
}
func (s *storeRecorder) Save() ([]byte, error) {
	s.record(action{name: "Save"})
	return nil, nil
}
func (s *storeRecorder) Recovery(b []byte) error {
	s.record(action{name: "Recovery"})
	return nil
}
func (s *storeRecorder) TotalTransactions() uint64 { return 0 }
func (s *storeRecorder) JsonStats() []byte         { return nil }
func (s *storeRecorder) DeleteExpiredKeys(cutoff time.Time) {
	s.record(action{
		name:   "DeleteExpiredKeys",
		params: []interface{}{cutoff},
	})
}

type stubWatcher struct{}

func (w *stubWatcher) EventChan() chan *store.Event { return nil }
func (w *stubWatcher) Remove()                      {}

// errStoreRecorder returns an store error on Get, Watch request
type errStoreRecorder struct {
	storeRecorder
	err error
}

func (s *errStoreRecorder) Get(_ string, _, _ bool) (*store.Event, error) {
	s.record(action{name: "Get"})
	return nil, s.err
}
func (s *errStoreRecorder) Watch(_ string, _, _ bool, _ uint64) (store.Watcher, error) {
	s.record(action{name: "Watch"})
	return nil, s.err
}

type waitRecorder struct {
	action []action
}

func (w *waitRecorder) Register(id int64) <-chan interface{} {
	w.action = append(w.action, action{name: fmt.Sprint("Register", id)})
	return nil
}
func (w *waitRecorder) Trigger(id int64, x interface{}) {
	w.action = append(w.action, action{name: fmt.Sprint("Trigger", id)})
}

func boolp(b bool) *bool { return &b }

func stringp(s string) *string { return &s }

type storageRecorder struct {
	recorder
}

func (p *storageRecorder) Save(st raftpb.HardState, ents []raftpb.Entry) {
	p.record(action{name: "Save"})
}
func (p *storageRecorder) Cut() error {
	p.record(action{name: "Cut"})
	return nil
}
func (p *storageRecorder) SaveSnap(st raftpb.Snapshot) {
	if raft.IsEmptySnap(st) {
		return
	}
	p.record(action{name: "SaveSnap"})
}

type readyNode struct {
	readyc chan raft.Ready
}

func newReadyNode() *readyNode {
	readyc := make(chan raft.Ready, 1)
	return &readyNode{readyc: readyc}
}
func (n *readyNode) Tick()                                          {}
func (n *readyNode) Campaign(ctx context.Context) error             { return nil }
func (n *readyNode) Propose(ctx context.Context, data []byte) error { return nil }
func (n *readyNode) ProposeConfChange(ctx context.Context, conf raftpb.ConfChange) error {
	return nil
}
func (n *readyNode) Step(ctx context.Context, msg raftpb.Message) error { return nil }
func (n *readyNode) Ready() <-chan raft.Ready                           { return n.readyc }
func (n *readyNode) ApplyConfChange(conf raftpb.ConfChange)             {}
func (n *readyNode) Stop()                                              {}
func (n *readyNode) Compact(d []byte)                                   {}

type nodeRecorder struct {
	recorder
}

func (n *nodeRecorder) Tick() {
	n.record(action{name: "Tick"})
}
func (n *nodeRecorder) Campaign(ctx context.Context) error {
	n.record(action{name: "Campaign"})
	return nil
}
func (n *nodeRecorder) Propose(ctx context.Context, data []byte) error {
	n.record(action{name: "Propose"})
	return nil
}
func (n *nodeRecorder) ProposeConfChange(ctx context.Context, conf raftpb.ConfChange) error {
	n.record(action{name: "ProposeConfChange"})
	return nil
}
func (n *nodeRecorder) Step(ctx context.Context, msg raftpb.Message) error {
	n.record(action{name: "Step"})
	return nil
}
func (n *nodeRecorder) Ready() <-chan raft.Ready { return nil }
func (n *nodeRecorder) ApplyConfChange(conf raftpb.ConfChange) {
	n.record(action{name: "ApplyConfChange"})
}
func (n *nodeRecorder) Stop() {
	n.record(action{name: "Stop"})
}
func (n *nodeRecorder) Compact(d []byte) {
	n.record(action{name: "Compact"})
}

type nodeProposeDataRecorder struct {
	nodeRecorder
	sync.Mutex
	d [][]byte
}

func (n *nodeProposeDataRecorder) data() [][]byte {
	n.Lock()
	d := n.d
	n.Unlock()
	return d
}
func (n *nodeProposeDataRecorder) Propose(ctx context.Context, data []byte) error {
	n.nodeRecorder.Propose(ctx, data)
	n.Lock()
	n.d = append(n.d, data)
	n.Unlock()
	return nil
}

type nodeProposalBlockerRecorder struct {
	nodeRecorder
}

func (n *nodeProposalBlockerRecorder) Propose(ctx context.Context, data []byte) error {
	<-ctx.Done()
	n.record(action{name: "Propose blocked"})
	return nil
}

type nodeConfChangeCommitterRecorder struct {
	nodeRecorder
	readyc chan raft.Ready
}

func newNodeConfChangeCommitterRecorder() *nodeConfChangeCommitterRecorder {
	readyc := make(chan raft.Ready, 1)
	readyc <- raft.Ready{SoftState: &raft.SoftState{RaftState: raft.StateLeader}}
	return &nodeConfChangeCommitterRecorder{readyc: readyc}
}
func (n *nodeConfChangeCommitterRecorder) ProposeConfChange(ctx context.Context, conf raftpb.ConfChange) error {
	data, err := conf.Marshal()
	if err != nil {
		return err
	}
	n.readyc <- raft.Ready{CommittedEntries: []raftpb.Entry{{Type: raftpb.EntryConfChange, Data: data}}}
	n.record(action{name: "ProposeConfChange:" + conf.Type.String()})
	return nil
}
func (n *nodeConfChangeCommitterRecorder) Ready() <-chan raft.Ready {
	return n.readyc
}
func (n *nodeConfChangeCommitterRecorder) ApplyConfChange(conf raftpb.ConfChange) {
	n.record(action{name: "ApplyConfChange:" + conf.Type.String()})
}

type waitWithResponse struct {
	ch <-chan interface{}
}

func (w *waitWithResponse) Register(id int64) <-chan interface{} {
	return w.ch
}
func (w *waitWithResponse) Trigger(id int64, x interface{}) {}

func mustClusterStore(t *testing.T, membs []Member) ClusterStore {
	c := Cluster{}
	if err := c.AddSlice(membs); err != nil {
		t.Fatalf("error creating cluster from %v: %v", membs, err)
	}
	return NewClusterStore(&getAllStore{}, c)
}
