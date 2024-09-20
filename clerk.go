package raft_lock

import (
	"crypto/rand"
	"cs350/labrpc"
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	events    chan Event
	leaderId  int32
	ClientID  ClientID
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd, events chan Event, clientId ClientID) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.ClientID = clientId
	ck.events = events
	ck.requestId = 0
	return ck
}

func (clerk *Clerk) ReceiveLock(args *ReceiveLockArgs, reply *ReceiveLockReply) {
	// log.Printf("%d received lock for key %s\n", clerk.ClientID, args.Key)
	clerk.events <- Event{eventType: ClientEvent, key: args.Key, id: clerk.ClientID, Op: AcquireLockOp}
}

// shared by Lock and Unlock.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("LockServer.SendMutex", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) SendMutex(key string, op OpType) {

	reqId := atomic.AddInt64(&ck.requestId, 1)
	leader := atomic.LoadInt32(&ck.leaderId)

	args := MutexArgs{
		Key:       key,
		Op:        op,
		ClientId:  ck.ClientID,
		RequestId: reqId,
	}

	server := leader
	for ; ; server = (server + 1) % int32(len(ck.servers)) {
		reply := MutexReply{}
		ok := ck.servers[server].Call("LockServer.Mutex", &args, &reply)

		if ok && reply.Err != ErrWrongLeader {
			break
		}
	}
	atomic.StoreInt32(&ck.leaderId, server)
}

func (ck *Clerk) Lock(key string) {
	ck.SendMutex(key, LockOp)
}
func (ck *Clerk) Unlock(key string) {
	ck.SendMutex(key, UnlockOp)
}
