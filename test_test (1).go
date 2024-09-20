package raft_lock

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).

func Lock(cfg *config, ck *Clerk, key string) {
	cfg.events <- Event{eventType: ClientEvent, key: key, id: ck.ClientID, Op: RequestLockOp}
	ck.Lock(key)
	cfg.op()
}

func Unlock(cfg *config, ck *Clerk, key string) {
	cfg.events <- Event{eventType: ClientEvent, key: key, id: ck.ClientID, Op: RequestUnlockOp}
	ck.Unlock(key)
	cfg.op()
}

func checkEvents(t *testing.T, events []Event) {
	DPrintf("Reached inside checkEvents")
	objs := map[string]bool{}
	for i, event := range events {
		DPrintf("Inside check events loop at iteration %d", i)
		if event.eventType == ClientEvent {
			DPrintf("EventType is ClientEvent, checking if event.Op is AcquireLockOp")
			if event.Op == AcquireLockOp {
				DPrintf("event.Op is AcquireLockOp, checking if objs[event.key]")
				if objs[event.key] {
					DPrintf("objs[event.key] is true. Deadlock incoming...")
					t.Fatalf("client event %v repeated", event)
				}
				objs[event.key] = true
			}
		} else {
			if event.Op == UnlockOp {
				objs[event.key] = false
			}
		}
	}
}

func TestBasicLocking(t *testing.T) {

	const nservers = 3
	const nclients = 2
	cfg := make_config(t, nservers, false, 100)

	defer cfg.cleanup()
	clients := make([]*Clerk, nclients)
	for i := range clients {
		clients[i] = cfg.makeClient(cfg.All())
	}
	events := []Event{}

	cfg.begin("Test: basic locking (4C)")
	wg := sync.WaitGroup{}
	go func() {
		for {
			event := <-cfg.events
			cfg.mu.Lock()
			events = append(events, event)
			cfg.mu.Unlock()
			if event.eventType == ServerEvent {
				wg.Done()
			}
			// log.Printf("event: %s", event.ToString())
		}
	}()

	wg.Add(1)
	Lock(cfg, clients[0], "k")
	wg.Add(1)
	Lock(cfg, clients[1], "k")
	wg.Add(1)
	Lock(cfg, clients[1], "j")
	wg.Add(1)
	Unlock(cfg, clients[0], "k")
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		cfg.mu.Lock()
		checkEvents(t, events)
		cfg.mu.Unlock()
	case <-time.After(20 * time.Second):
		t.Fatalf("locking failed")
	}

	cfg.end()
}
func TestLockingUnreliable(t *testing.T) {

	const nservers = 3
	const nclients = 2
	cfg := make_config(t, nservers, true, 100)

	defer cfg.cleanup()
	clients := make([]*Clerk, nclients)
	for i := range clients {
		clients[i] = cfg.makeClient(cfg.All())
	}

	// objects := []string{"a", "b", "c", "d"}
	events := []Event{}

	cfg.begin("Test: unreliable locking (4C)")
	wg := sync.WaitGroup{}
	go func() {
		for {
			event := <-cfg.events
			cfg.mu.Lock()
			events = append(events, event)
			cfg.mu.Unlock()
			if event.eventType == ServerEvent {
				wg.Done()
			}
			// log.Printf("event: %s", event.ToString())
		}
	}()

	wg.Add(1)
	Lock(cfg, clients[0], "k")
	wg.Add(1)
	Lock(cfg, clients[1], "k")
	wg.Add(1)
	Lock(cfg, clients[1], "j")
	wg.Add(1)
	Unlock(cfg, clients[0], "k")

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		cfg.mu.Lock()
		checkEvents(t, events)
		cfg.mu.Unlock()
	case <-time.After(10 * time.Second):
		t.Fatalf("locking failed")
	}

	cfg.end()
}

func TestMoreLocking(t *testing.T) {

	const nservers = 3
	const nclients = 4
	const attempts = 10
	objects := []string{"a", "b", "c", "d"}
	cfg := make_config(t, nservers, false, nclients*len(objects)*attempts)

	defer cfg.cleanup()
	clients := make([]*Clerk, nclients)
	for i := range clients {
		clients[i] = cfg.makeClient(cfg.All())
	}

	events := []Event{}

	cfg.begin("Test: more locking (4C)")
	wg := sync.WaitGroup{}
	go func() {
		for {
			event := <-cfg.events
			cfg.mu.Lock()
			events = append(events, event)
			cfg.mu.Unlock()
			// log.Printf("TESTER event: %s", event.ToString())
			if event.eventType == ServerEvent {
				wg.Done()
			}
		}
	}()

	records := make(map[int64]map[string]int)
	for i := 0; i < nclients; i++ {
		records[clients[i].ClientID] = make(map[string]int)
	}
	for i := 0; i < len(objects)*attempts; i++ {
		for j := 0; j < nclients; j++ {
			wg.Add(1)
			randomObj := objects[rand.Intn(len(objects))]
			if v, ok := records[clients[j].ClientID][randomObj]; ok && v > 0 {
				if rand.Int()%4 == 0 {
					Lock(cfg, clients[j], randomObj)
					records[clients[j].ClientID][randomObj] += 1
				} else {
					Unlock(cfg, clients[j], randomObj)
					records[clients[j].ClientID][randomObj] -= 1
				}
			} else {
				Lock(cfg, clients[j], randomObj)
				records[clients[j].ClientID][randomObj] = 1
			}
		}
	}
	for i := 0; i < nclients; i++ {
		for obj, v := range records[clients[i].ClientID] {
			for v > 0 {
				wg.Add(1)
				Unlock(cfg, clients[i], obj)
				v--
			}
		}
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		DPrintf("case <- doneCh")
		cfg.mu.Lock()
		DPrintf("Locked cfg.mu, calling checkEvents")
		checkEvents(t, events)
		DPrintf("Finished checking events, unlocking cfg.mu")
		cfg.mu.Unlock()
		DPrintf("cfg.mu has been Unlocked")
	case <-time.After(80 * time.Second):
		t.Fatalf("test timed out")
	}

	cfg.end()
}
