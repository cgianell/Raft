package raft_lock

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type ClientID = int64

type OpType int

const (
	LockOp          OpType = 1
	UnlockOp        OpType = 2
	RequestLockOp   OpType = 3
	AcquireLockOp   OpType = 4
	RequestUnlockOp OpType = 5
)

func (o OpType) ToString() string {
	switch o {
	case LockOp:
		return "Lock"
	case UnlockOp:
		return "Unlock"
	case RequestLockOp:
		return "RequestLock"
	case AcquireLockOp:
		return "AcquireLock"
	case RequestUnlockOp:
		return "RequestUnlock"
	}
	panic("impossible!")
}

type MutexArgs struct {
	Key string
	Op  OpType
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  ClientID
	RequestId int64
}

type MutexReply struct {
	Err string
}

type ReceiveLockArgs struct {
	Key       string
	RequestId int64
}

type ReceiveLockReply struct {
}

type EventType int

const (
	ServerEvent EventType = 1
	ClientEvent EventType = 2
)

func (e EventType) ToString() string {
	switch e {
	case ServerEvent:
		return "Server"
	case ClientEvent:
		return "Client"
	}
	panic("impossible!")
}

type Event struct {
	Op        OpType
	eventType EventType
	key       string
	id        ClientID
}

func (e Event) ToString() string {
	return fmt.Sprintf("Event: %s %s %v %v", e.eventType.ToString(), e.Op.ToString(), e.key, e.id)
}
