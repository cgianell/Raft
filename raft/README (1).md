# Project 4C: Raft based Distributed Lock Service (Bonus)


## Introduction
The goal of this assignment is to see how you can use Raft to build a real world application.This involves working with Client and Server side code, which interacts with Raft under the hood. You will have to apply commands according to the application logic and update its state. 


Specifically, you will be implementing a Distributed Lock Service using Raft. You will make use of the Raft implementation you worked on in assignment 4B.


A distributed lock service allows multiple clients to acquire locks on objects in order to avoid data races. These clients could be running on completely independent servers, and as such using a regular lock might not work out.


The `Client` is responsible for sending `Lock` and `Unlock` `Operations` to the `Server` for a given `Object` and `User`. The server must submit this command to the Raft service by calling the `Start()` method. Once Raft successfully commits the Operation, the server should inform the client that their request has been committed. The server should also keep checking if a requested Lock is now available for acquisition, and should inform the Client who requested this lock by calling the `ReceiveLock()` RPC.


## Getting Started


You are already provided with the Client and most of the Server implementations. Here is a run down of what different methods in the server do.


### Entry Point


`Mutex()`


- Receives an RPC from `Client` to acquire lock on a given key
- calls `waitForApplied()`


`waitForApplied()`


- Submits a command to Raft
- Waits for applyOpsLoop to send a message on the OpCh channel telling if Raft has applied (committed) the command
- Times out after 600ms
- Tells the client that their request has been committed (the lock may or may not be acquired yet)


`applyOpsLoop()`


- Runs in the background (starts on server startup)
- Keeps checking if Raft has committed something
- If Raft has committed something, tell the corresponding client that the commit is done
- Apply the command to the servers State Machine via `applyToStateMachine()`


`applyToStateMachine()`


- Called when Raft commits an operation, receives the operation as a parameter
- Gets the `originalLockOwner` (the Id at the first index of the `LockStore`, if it exists, else reply with `doesntExist`)
- Applies the operation to the `LockStore` queue
- Checks if after adding to the `LockStore` queue, for the given `Key`, if the `Owner` has changed.
-  `if newLockOwner != originalLockOwner` -> call `giveLockToOwner()` (see examples)


`giveLockToOwner()`


- Calls the `ReceiveLock` RPC on the Client
- Client does whatever they want to do now :D


### Examples
1. Locking:
  - The client tries to acquire a lock for Key1 by User1.
  - Currently, the LockStore queue looks like this `{Key1: []}`
  - Currently, no owner exists (ie, `originalLockOwner` = `nil`)
  - After applying the operation `Lock Key1 by User1`, check `LockStore` queue for `Key1`
    - Now, the LockStore queue looks like this `{Key1: ['User1]}`
  - Now, `User1` is the first in queue for `Key1`
    - ie. `newLockOwner = User1`
  - ie `if newLockOwner != originalLockOwner` -> `giveLockToOwner()` [ give lock to `User1` ]


2. Unlocking:
  - Assume that `User1` has the Lock for `Key1`, and `User2` has also submitted a request for the same Lock
  - Currently, the LockStore queue looks like this `{Key1: [User1, User2]}`
  - The client unlocks Key1 for User1.
  - Currently, the owner is User1 (ie, `originalLockOwner` = `User1`)
  - After applying the operation `Unlock Key1 by User1`, check `LockStore` queue for `Key1`
    - Now, the LockStore queue looks like this `{Key1: ['User2]}`
  - Now, `User2` is the first in queue for `Key1` (since `User1` got removed)
    - ie. `newLockOwner = User2`
  - ie `if newLockOwner != originalLockOwner` -> `giveLockToOwner()` [ give lock to `User2` ]



## Your Task
Implement the `applyToStateMachine()` function. You may use any of the available methods in `ls.store` such as `ls.store.getLockOwner()`. You are also required to send an `Event` (see `common.go`) to the `ls.events` channel. This would allow the tester to confirm the order in which your server is applying events.




## Testing the code
```
$ cd raft-lock
$ go test -race
```

Here is the typical output
```
$ go test -race
Test: basic locking (4C) ...
  ... Passed --   0.3  3  1536    4
Test: unreliable locking (4C) ...
  ... Passed --   0.9  3   123    4
Test: more locking (4C) ...
  ... Passed --   1.7  3  3121  178
PASS
ok  	cs350/raft-lock	4.188s
```


# Hints
- Refer to `RaftLockFlowDiagram.png` for an eagle eye view of how the client and server interact.


All the best!
