package raft_lock

import (
	"os"
	"testing"

	"cs350/labrpc"

	// import "log"
	crand "crypto/rand"
	"cs350/raft"
	"encoding/base64"
	"fmt"
	"math/big"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// Randomize server handles
func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
	sa := make([]*labrpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

type config struct {
	mu           sync.Mutex
	t            *testing.T
	net          *labrpc.Network
	events       chan Event
	n            int
	lockServers  []*LockServer
	saved        []*raft.Persister
	endnames     [][]string // names of each server's sending ClientEnds
	clerks       map[*Clerk][]string
	clerkEnds    map[ClientID]*labrpc.ClientEnd
	nextClientId int
	start        time.Time // time at which make_config() was called
	// begin()/end() statistics
	t0    time.Time // time at which test_test.go called cfg.begin()
	rpcs0 int       // rpcTotal() at start of test
	ops   int32     // number of clerk get/put/append method calls
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	DPrintf("Reached cleanup, about to clean up")
	cfg.mu.Lock()
	DPrintf("Locked cfg.mu")
	defer cfg.mu.Unlock()
	DPrintf("Deferred unlock cfg.mu")
	for i := 0; i < len(cfg.lockServers); i++ {
		DPrintf("Inside loop at iteration %d", i)
		if cfg.lockServers[i] != nil {
			DPrintf("Found a non-nill lockServer")
			cfg.lockServers[i].Kill()
			DPrintf("Killed non-nill lockServer")
		}
	}
	DPrintf("Done with regular clean-up, calling net.Cleanup")
	cfg.net.Cleanup()
	DPrintf("Done with net clean-up, calling checkTimeout")
	cfg.checkTimeout()
	DPrintf("Done with everything, unlocking cfg.mu")
}

// Maximum log size across all servers
func (cfg *config) LogSize() int {
	logsize := 0
	for i := 0; i < cfg.n; i++ {
		n := cfg.saved[i].RaftStateSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}

// attach server i to servers listed in to
// caller must hold cfg.mu
func (cfg *config) connectUnlocked(i int, to []int) {
	// log.Printf("connect peer %d to %v\n", i, to)

	// outgoing socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[i][to[j]]
		cfg.net.Enable(endname, true)
	}

	// incoming socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[to[j]][i]
		cfg.net.Enable(endname, true)
	}
}

func (cfg *config) connect(i int, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.connectUnlocked(i, to)
}

// detach server i from the servers listed in from
// caller must hold cfg.mu
func (cfg *config) disconnectUnlocked(i int, from []int) {
	// log.Printf("disconnect peer %d from %v\n", i, from)

	// outgoing socket files
	for j := 0; j < len(from); j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][from[j]]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming socket files
	for j := 0; j < len(from); j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[from[j]][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) disconnect(i int, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.disconnectUnlocked(i, from)
}

func (cfg *config) All() []int {
	all := make([]int, cfg.n)
	for i := 0; i < cfg.n; i++ {
		all[i] = i
	}
	return all
}

func (cfg *config) ConnectAll() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < cfg.n; i++ {
		cfg.connectUnlocked(i, cfg.All())
	}
}

func (cfg *config) addClerkToNetwork(clerk *Clerk) { // in order to allow RPCs back from servers for unlock responses
	cfg.net.Connect(clerk.ClientID, clerk.ClientID)
	cfg.net.Enable(clerk.ClientID, true)
	clerksv := labrpc.MakeService(clerk)
	srv := labrpc.MakeServer()
	srv.AddService(clerksv)
	cfg.net.AddServer(clerk.ClientID, srv)
}

// Create a clerk with clerk specific server names.
// Give it connections to all of the servers, but for
// now enable only connections to servers in to[].
func (cfg *config) makeClient(to []int) *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	clientID := nrand()
	clerkEnd := cfg.net.MakeEnd(clientID)
	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	endnames := make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		endnames[j] = randstring(20)
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], j)
	}

	// for
	ck := MakeClerk(random_handles(ends), cfg.events, clientID)
	cfg.clerks[ck] = endnames
	cfg.clerkEnds[ck.ClientID] = clerkEnd
	cfg.nextClientId++
	cfg.ConnectClientUnlocked(ck, to)
	return ck
}

func (cfg *config) deleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.clerks, ck)
	delete(cfg.clerkEnds, ck.ClientID)
}

// caller should hold cfg.mu
func (cfg *config) ConnectClientUnlocked(ck *Clerk, to []int) {
	// log.Printf("ConnectClient %v to %v\n", ck, to)
	cfg.addClerkToNetwork(ck)
	endnames := cfg.clerks[ck]
	for j := 0; j < len(to); j++ {
		s := endnames[to[j]]
		cfg.net.Enable(s, true)
	}
}

func (cfg *config) ConnectClient(ck *Clerk, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.ConnectClientUnlocked(ck, to)
}

// caller should hold cfg.mu
func (cfg *config) DisconnectClientUnlocked(ck *Clerk, from []int) {
	// log.Printf("DisconnectClient %v from %v\n", ck, from)
	endnames := cfg.clerks[ck]
	for j := 0; j < len(from); j++ {
		s := endnames[from[j]]
		cfg.net.Enable(s, false)
	}
}

func (cfg *config) DisconnectClient(ck *Clerk, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.DisconnectClientUnlocked(ck, from)
}

// Shutdown a server by isolating it
func (cfg *config) ShutdownServer(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	cfg.disconnectUnlocked(i, cfg.All())

	// disable client connections to the server.
	// it's important to do this before creating
	// the new Persister in saved[i], to avoid
	// the possibility of the server returning a
	// positive reply to an Append but persisting
	// the result in the superseded Persister.
	cfg.net.DeleteServer(i)

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	kv := cfg.lockServers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		cfg.lockServers[i] = nil
	}
}

// If restart servers, first call ShutdownServer
func (cfg *config) StartServer(i int) {
	cfg.mu.Lock()

	// a fresh set of outgoing ClientEnd names.
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// give the fresh persister a copy of the old persister's
	// state, so that the spec is that we pass StartLockServer()
	// the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = raft.MakePersister()
	}
	cfg.mu.Unlock()
	cfg.lockServers[i] = StartLockServer(ends, cfg.clerkEnds, cfg.events, i, cfg.saved[i])

	kvsvc := labrpc.MakeService(cfg.lockServers[i])
	rfsvc := labrpc.MakeService(cfg.lockServers[i].rf)
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) Leader() (bool, int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	for i := 0; i < cfg.n; i++ {
		_, is_leader := cfg.lockServers[i].rf.GetState()
		if is_leader {
			return true, i
		}
	}
	return false, 0
}

var ncpu_once sync.Once

func make_config(t *testing.T, n int, unreliable bool, eventSize int) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.events = make(chan Event, eventSize)
	cfg.lockServers = make([]*LockServer, cfg.n)
	cfg.saved = make([]*raft.Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.clerks = make(map[*Clerk][]string)
	cfg.clerkEnds = make(map[ClientID]*labrpc.ClientEnd)
	cfg.nextClientId = cfg.n + 1000 // client ids start 1000 above the highest serverid
	cfg.start = time.Now()

	// create a full set of Lock servers.
	for i := 0; i < cfg.n; i++ {
		cfg.StartServer(i)
	}

	cfg.ConnectAll()

	cfg.net.Reliable(!unreliable)

	return cfg
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

// start a Test.
// print the Test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	atomic.StoreInt32(&cfg.ops, 0)
}

func (cfg *config) op() {
	atomic.AddInt32(&cfg.ops, 1)
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		t := time.Since(cfg.t0).Seconds()  // real time
		npeers := cfg.n                    // number of Raft peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0 // number of RPC sends
		ops := atomic.LoadInt32(&cfg.ops)  //  number of clerk lock/unlock calls

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %5d %4d\n", t, npeers, nrpc, ops)
	}
}
