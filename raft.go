package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()PrevLogIndex

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int32
	votedFor      int32
	log           []LogEntry
	electionTimer *time.Timer

	commitIndex int32
	lastApplied int32

	nextIndex  []int
	matchIndex []int

	replicating bool

	// State needed for 4A
	state int32
	// voteCount   int32
	heartbeatCh chan AppendEntriesArgs
	clientCh    chan struct{}
	applyCh     chan ApplyMsg
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int32      // Leader's term
	LeaderId     int        // So follower can redirect clients
	PrevLogIndex int32      // Index of log entry immediately preceding new ones
	PrevLogTerm  int32      // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int32      // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int32 // CurrentTerm, for leader to update itself
	Success       bool  // True if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int32
	ConflictTerm  int32
	MatchIndex    int // Index of the last commit of the server
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	logShow := make([]int, len(rf.log))
	if len(rf.log) > 0 {
		for i := range rf.log {
			logShow[i] = (rf.log[i].Term)
		}
	}
	//log.Printf("Raft %d got entries %d for term %d. Has logs %d", rf.me, args.Entries, rf.currentTerm, logShow)
	rf.mu.Unlock()

	// Reset election timer and handle heartbeat if term is valid
	if atomic.LoadInt32(&args.Term) >= atomic.LoadInt32(&rf.currentTerm) {
		rf.resetElectionTimer()
		rf.heartbeatCh <- *args
		if atomic.LoadInt32(&args.Term) > atomic.LoadInt32(&rf.currentTerm) {
			rf.mu.Lock()
			atomic.StoreInt32(&rf.currentTerm, atomic.LoadInt32(&args.Term))
			atomic.StoreInt32(&rf.votedFor, -1)
			atomic.StoreInt32(&rf.state, Follower)
			rf.persist()
			rf.mu.Unlock()
		}
	}

	// disconnect := (args.LeaderCommit >= atomic.LoadInt32(&rf.commitIndex) && args.Term < atomic.LoadInt32(&rf.currentTerm))

	// if disconnect {
	// 	atomic.StoreInt32(&rf.state, Follower)
	// 	atomic.StoreInt32(&rf.currentTerm, args.Term)
	// 	atomic.AddInt32(&rf.votedFor, -1)
	// 	rf.persist()
	// }

	rf.mu.Lock()
	lastLogIndex := len(rf.log) - 1
	rf.mu.Unlock()
	// Early exit if the incoming term is less than the current term
	if atomic.LoadInt32(&args.Term) < atomic.LoadInt32(&rf.currentTerm) {
		reply.Success = false
		atomic.StoreInt32(&reply.Term, atomic.LoadInt32(&rf.currentTerm))
		atomic.StoreInt32(&reply.ConflictIndex, int32(lastLogIndex))
		rf.mu.Lock()
		atomic.StoreInt32(&reply.ConflictTerm, int32(rf.log[lastLogIndex].Term))
		rf.mu.Unlock()
		return
	}

	reply.Term = atomic.LoadInt32(&args.Term)
	rf.mu.Lock()
	if atomic.LoadInt32(&args.PrevLogIndex) > int32(lastLogIndex) || int32(rf.log[atomic.LoadInt32(&args.PrevLogIndex)].Term) != atomic.LoadInt32(&args.PrevLogTerm) {
		reply.Success = false
		atomic.StoreInt32(&reply.Term, atomic.LoadInt32(&rf.currentTerm))
		if atomic.LoadInt32(&args.PrevLogIndex) > int32(lastLogIndex) {
			atomic.StoreInt32(&reply.ConflictIndex, int32(lastLogIndex+1))
			atomic.StoreInt32(&reply.ConflictTerm, -1)
		} else {
			atomic.StoreInt32(&reply.ConflictTerm, int32(rf.log[atomic.LoadInt32(&args.PrevLogIndex)].Term))
			// Find first index with this term to help leader optimize next AppendEntries
			for i := atomic.LoadInt32(&args.PrevLogIndex); atomic.LoadInt32(&i) >= 0; atomic.AddInt32(&i, -1) {
				if int32(rf.log[i].Term) == atomic.LoadInt32(&reply.ConflictTerm) {
					atomic.StoreInt32(&reply.ConflictIndex, atomic.LoadInt32(&i)+1)
				}
			}
		}
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	rf.mu.Lock()
	rf.log = rf.log[:atomic.LoadInt32(&args.PrevLogIndex)+1]
	rf.mu.Unlock()
	rf.mu.Lock()
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	rf.mu.Unlock()

	// 5. Update commit index
	if atomic.LoadInt32(&args.LeaderCommit) > atomic.LoadInt32(&rf.commitIndex) {
		rf.mu.Lock()
		atomic.StoreInt32(&rf.commitIndex, int32(min(int(atomic.LoadInt32(&args.LeaderCommit)), len(rf.log)-1)))
		//log.Printf("***leader commit %d follower %d commitindex: %d", args.LeaderCommit, rf.me, rf.commitIndex)
		rf.mu.Unlock()
	}

	if atomic.LoadInt32(&rf.lastApplied) < atomic.LoadInt32(&args.PrevLogIndex) {
		rf.mu.Lock()
		rf.applyCommitted()
		rf.mu.Unlock()
	}
	reply.Success = true
}

// applyCommitted sends committed log entries to the applyCh.
func (rf *Raft) applyCommitted() {
	for atomic.LoadInt32(&rf.lastApplied) < atomic.LoadInt32(&rf.commitIndex) {
		atomic.AddInt32(&rf.lastApplied, 1)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[atomic.LoadInt32(&rf.lastApplied)].Command,
			CommandIndex: int(atomic.LoadInt32(&rf.lastApplied)),
		}
		rf.applyCh <- applyMsg
		//log.Printf("****Raft %d applied msg command %d at index %d", rf.me, rf.log[rf.lastApplied], applyMsg.CommandIndex)
	}
}

// Send
func (rf *Raft) replicateLogs(leadCommmit int32) {
	// Make local copies of the data needed for the goroutine
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastTerm := int32(rf.log[len(rf.log)-1].Term)
	if atomic.LoadInt32(&rf.state) != Leader || atomic.LoadInt32(&rf.currentTerm) > atomic.LoadInt32(&lastTerm) {
		return
	}

	for i := range rf.peers {

		if i == rf.me {
			atomic.StoreInt32(&leadCommmit, atomic.LoadInt32(&rf.commitIndex))
			continue
		}
		if i != rf.me && len(rf.log)-1 < rf.nextIndex[i] {
			continue // Skip the leader itself.
		}
		// Send AppendEntries RPCs asynchronously.
		go func(server int) {
			rf.mu.Lock()
			prevLogIndex := int32(rf.nextIndex[server] - 1)
			prevLogTerm := int32(0)
			if atomic.LoadInt32(&prevLogIndex) >= 0 && atomic.LoadInt32(&prevLogIndex) < int32(len(rf.log)) {
				atomic.StoreInt32(&prevLogTerm, int32(rf.log[prevLogIndex].Term))
			}
			lastTermin := int32(rf.log[len(rf.log)-1].Term)
			if atomic.LoadInt32(&rf.state) != Leader || atomic.LoadInt32(&rf.currentTerm) > atomic.LoadInt32(&lastTermin) {
				return
			}

			entries := rf.log[rf.nextIndex[server]:]
			if rf.nextIndex[server] < int(prevLogIndex)+1 {
				entries = rf.log[rf.nextIndex[server] : prevLogIndex+1] // [nextIndex, index+1)
			}
			rf.mu.Unlock()
			args := AppendEntriesArgs{
				Term:         atomic.LoadInt32(&rf.currentTerm),
				LeaderId:     rf.me,
				PrevLogIndex: atomic.LoadInt32(&prevLogIndex),
				PrevLogTerm:  atomic.LoadInt32(&prevLogTerm),
				Entries:      entries,
				LeaderCommit: atomic.LoadInt32(&leadCommmit),
			}

			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				//log.Printf("entries %d sent to raft: %d.", args.Entries, server)
				// Check if AppendEntries failed due to log inconsistency.
				rf.handleAppendEntriesResponse(server, &args, &reply)
				// rf.commitIndex == leadCommmit ||
				// if reply.Success {
				// 	break
				// }
				// // }
			} else {
				return
			}
		}(i)
	}
}

func (rf *Raft) handleAppendEntriesResponse(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		//log.Printf("=====Changing next and match indexes for server %d ===", server)
		////log.Printf("---Raft: %d Next index= %d, match index= %d", rf.me, rf.nextIndex, rf.matchIndex)
		// If successful, update nextIndex and matchIndex.
		rf.mu.Lock()
		rf.nextIndex[server] = int(atomic.LoadInt32(&args.PrevLogIndex)) + len(args.Entries) + 1
		rf.matchIndex[server] = int(atomic.LoadInt32(&args.PrevLogIndex)) + len(args.Entries)
		rf.mu.Unlock()
		rf.updateCommitIndex()
		//log.Printf("---Raft: %d Next index= %d, match index= %d", rf.me, rf.nextIndex, rf.matchIndex)
		// rf.replicating = false
		// rf.clientCh <- struct{}{} // Signal that replication is done
	} else {
		// On failure, reduce the nextIndex using a binary search approach.
		if reply.ConflictIndex > 0 {
			// If the follower returned a specific conflict index, use it to refine the search.
			rf.mu.Lock()
			rf.nextIndex[server] = int(reply.ConflictIndex)
			rf.mu.Unlock()
		} else {
			// If no specific conflict index is provided, halve the distance to the base index.
			rf.mu.Lock()
			lastKnownGoodIndex := max(1, rf.matchIndex[server])
			currentNextIndex := rf.nextIndex[server]
			rf.nextIndex[server] = lastKnownGoodIndex + (currentNextIndex-lastKnownGoodIndex)/2
			rf.nextIndex[server] = 1
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		rf.nextIndex[server] = max(1, rf.nextIndex[server]) // Ensure nextIndex doesn't go below 1 or the base index.
		rf.mu.Unlock()

		//log.Printf("---Raft: %d Next index= %d, match index= %d", rf.me, rf.nextIndex, rf.matchIndex)

	}
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (4B).
	index := int32(-1)
	term := int32(-1)
	isLeader := (atomic.LoadInt32(&rf.state) == Leader)

	if !isLeader {
		return -1, int(atomic.LoadInt32(&rf.currentTerm)), false
	}

	//TOCHECK
	// Append the command to the log as a new entry.
	// prevLogIndex := len(rf.log) - 1
	logEntry := LogEntry{Term: int(atomic.LoadInt32(&rf.currentTerm)), Command: command}
	rf.mu.Lock()
	index = int32(len(rf.log)) // Placeholder for actual log position computation
	//log.Printf("Commit Index= %d", rf.commitIndex)
	////log.Printf("Leader: %d appending entries... Leader log before update: %d.", rf.me, rf.log)
	rf.log = append(rf.log, logEntry)
	// terms := make([]int, len(rf.log))
	// for i := range rf.log {
	// 	terms[i] = rf.log[i].Term
	// }
	//log.Printf("Leader %d with logs: %d. after append, my term: %d", rf.me, rf.log, rf.currentTerm)
	rf.mu.Unlock()
	// term = int(atomic.LoadInt32(&rf.currentTerm)) // TODO: Dont forget to change this for part B
	// Append new log entry (placeholder, real implementation would handle log replication)
	// rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	rf.mu.Lock()
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log)
	// Update persistent state before returning.
	rf.persist()
	rf.mu.Unlock()

	// leadCommmit := atomic.LoadInt32(&rf.commitIndex)
	select {
	case rf.clientCh <- struct{}{}:
	default:
	}

	term = atomic.LoadInt32(&rf.currentTerm)

	//log.Printf("returning index: %d term %d", index, term)
	return int(index), int(term), isLeader
}

// updateCommitIndex checks if there is a majority of matchIndex[i] >= N
// where N is the index of the last new entry in log, and commits the entries.
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	N := len(rf.log) - 1
	//log.Printf("___ Raft %d CommitIndex: %d AppliedIdx: %d", rf.me, rf.commitIndex, rf.lastApplied)
	// added last or
	for ; N > int(atomic.LoadInt32(&rf.commitIndex)) && (int32(rf.log[N].Term) >= atomic.LoadInt32(&rf.currentTerm) || int32(rf.log[N].Term) > rf.votedFor); N-- {
		count := 1 // count the leader
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			// Majority found, update commitIndex
			atomic.StoreInt32(&rf.commitIndex, int32(N))
			//log.Printf("----CommitIndex: %d AppliedIdx: %d", rf.commitIndex, rf.lastApplied)
			rf.applyCommitted()
			break
		}
	}
	rf.mu.Unlock()
}

const (
	Follower           int32 = 0
	Candidate          int32 = 1
	Leader             int32 = 2
	ElectionTimeoutMin       = 300                   // milliseconds
	ElectionTimeoutMax       = 500                   // milliseconds
	HeartbeatInterval        = 50 * time.Millisecond // 50 ms
)

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//var term int
	//var isleader bool
	// Your code here (4A).
	return int(atomic.LoadInt32(&rf.currentTerm)), atomic.LoadInt32(&rf.state) == Leader
	//return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(atomic.LoadInt32(&rf.currentTerm))
	e.Encode(atomic.LoadInt32(&rf.votedFor))
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int32
	var votedFor int32
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		fmt.Println(errors.New("failed to restore persisted state"))
	} else {
		//log.Printf("Restored persistent state, old term:%d", rf.currentTerm)
		atomic.StoreInt32(&rf.currentTerm, atomic.LoadInt32(&currentTerm))
		//log.Printf("updated term: %d", rf.currentTerm)
		atomic.StoreInt32(&rf.votedFor, atomic.LoadInt32(&votedFor))
		//log.Printf("Restored persistent state, old logs:%d", rf.log)
		rf.log = logs
		//log.Printf("updated logs: %d", rf.log)
	}
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int32
	CandidateId  int
	LastLogIndex int32
	LastLogTerm  int32
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int32
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//log.Printf("Candidate %d asking %d for votes", args.CandidateId, rf.me)

	// Candidate is out of date
	if atomic.LoadInt32(&args.Term) > atomic.LoadInt32(&rf.currentTerm) { // Check if the term in the heartbeat came from a new leader if so
		atomic.StoreInt32(&rf.state, Follower)
		atomic.StoreInt32(&rf.currentTerm, args.Term)
		atomic.StoreInt32(&rf.votedFor, -1)
		rf.mu.Lock()
		rf.persist()
		rf.mu.Unlock()
		//log.Printf("Raft %d: Handling RequestVote from %d for term %d. Raft %d demoted to follower", rf.me, args.CandidateId, args.Term, rf.me)
	}

	reply.Term = atomic.LoadInt32(&rf.currentTerm)

	// if atomic.LoadInt32(&rf.commitIndex) > int32(args.LastLogIndex)
	// if atomic.LoadInt32(&rf.commitIndex) < int32(args.LastLogIndex) {
	// 	// Candidate's term is newer, update from persistent state first
	// 	rf.readPersist(rf.persister.ReadRaftState())
	// 	atomic.StoreInt32(&rf.state, Follower)
	//log.Printf("Raft %d disconnected, recovering...", rf.me)
	//log.Printf("Raft %d: Handling RequestVote from %d for term %d, voteGranted: %t, my term %d, votedfor: %d", rf.me, args.CandidateId, args.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
	// }

	// Grant vote if (1) rf hasn't voted this term or (2) already voted for this candidate, and
	// (3) the candidate's log is at least as up-to-date as rf's log.
	//  TODO: Check if next line needed
	// upToDate := rf.logIsUpToDate(args.LastLogIndex, args.LastLogTerm)
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm() // Useful for log replication process for nodes to check Candidate's log's uptodate~ness
	upToDate := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (atomic.LoadInt32(&rf.votedFor) == -1 || atomic.LoadInt32(&rf.votedFor) == int32(args.CandidateId)) && upToDate {
		reply.VoteGranted = true
		atomic.StoreInt32(&rf.votedFor, int32(args.CandidateId))
		rf.mu.Lock()
		// maybe not needed?
		rf.resetElectionTimer()
		rf.persist()
		rf.mu.Unlock()
		// rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}

	//log.Printf("Raft %d: Handling RequestVote from %d for term %d, voteGranted: %t, my term %d, votedfor: %d", rf.me, args.CandidateId, args.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
}

func (rf *Raft) resetElectionTimer() {
	// Stop the current timer and drain the channel if necessary
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
	timeout := ElectionTimeoutMin + rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)
	rf.electionTimer.Reset(time.Duration(timeout) * time.Millisecond)

}

func (rf *Raft) startElection() {
	atomic.StoreInt32(&rf.state, Candidate)
	currentTerm := atomic.AddInt32(&rf.currentTerm, 1)
	rf.mu.Lock() // Whoever started the election is now a Candidate
	//log.Printf("Raft %d: State changed to Candidate, starting election at term %d", rf.me, rf.currentTerm) // Candidate changes the term for itself since it may become the new leader
	atomic.StoreInt32(&rf.votedFor, int32(rf.me)) // Candidate votes for itself
	rf.persist()
	rf.mu.Unlock()
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm() // Useful for log replication process for nodes to check Candidate's log's uptodate~ness

	var votesReceived int32 = 1 // Start with a self-vote
	for i := range rf.peers {   // For all nodes
		if i == rf.me {
			continue
		}
		// Except myself
		go func(server int) { // Send Vote Request
			args := RequestVoteArgs{
				Term:         atomic.LoadInt32(&currentTerm),
				CandidateId:  rf.me,
				LastLogIndex: atomic.LoadInt32(&lastLogIndex),
				LastLogTerm:  atomic.LoadInt32(&lastLogTerm),
			}
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				if !reply.VoteGranted {
					// atomic.StoreInt32(&rf.state, Follower)
					// atomic.StoreInt32(&rf.currentTerm, reply.Term)
					// atomic.StoreInt32(&rf.votedFor, -1)
					// rf.persist()
					// return
				} else if reply.VoteGranted && atomic.LoadInt32(&rf.state) == Candidate && atomic.LoadInt32(&currentTerm) == atomic.LoadInt32(&rf.currentTerm) {
					atomic.AddInt32(&votesReceived, 1)
					if atomic.LoadInt32(&votesReceived) > int32(len(rf.peers)/2) {
						rf.convertToLeader()
						//log.Printf("Raft %d became LEADER", rf.me)
					}

				}
			} else {
				// //log.Printf("Failed election RPC")
				return
			}
		}(i)
		time.Sleep(10 * time.Millisecond)
	}
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//log.Printf("Raft %d: Sent RequestVote to Raft: %d for term %d", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// if args.Entries != nil {
	//log.Printf("Raft %d with logs: %d. Sent AppendEntries to Raft: %d with entries: %d, term: %d", rf.me, rf.log, server, args.Entries, args.Term)
	// }
	// ch := make(chan bool, 1)

	// // Launch a goroutine to make the RPC call
	// go func() {
	// 	ch <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// }()

	// // Wait for the RPC call to complete or timeout
	// select {
	// case ok := <-ch:
	// 	return ok
	// case <-time.After(500 * time.Millisecond): // Timeout after 500 milliseconds
	// 	return false
	// }
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for { // Loop forever (while the node is alive)
		// rf.mu.Lock()
		if rf.killed() {
			// rf.mu.Unlock()
			return
		}
		state := atomic.LoadInt32(&rf.state) // Update the state constantly
		// rf.mu.Unlock()
		switch atomic.LoadInt32(&state) { // Check for node state first
		case Leader: // If we are a leader
			select {
			case <-rf.clientCh:
				rf.replicateLogs(atomic.LoadInt32(&rf.commitIndex))
				time.Sleep(10 * time.Millisecond)
			default:
				time.Sleep(10 * time.Millisecond)
			}
			rf.sendHeartbeats() // Send heartbeats constantly
			time.Sleep(HeartbeatInterval)
		default: // If we are not a leader
			select { // Then check our timer constantly
			case <-rf.electionTimer.C: // If the election timer times out
				rf.startElection()      // Start a new election
				rf.resetElectionTimer() // and reset the timer
			case <-rf.heartbeatCh: // If we receive a heartbeat at any time
				rf.resetElectionTimer() // no need to start a new election
			default:
				// Avoid blocking the goroutine
				time.Sleep(HeartbeatInterval)
			}
		}
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,

		// Initialize your data here (4A, 4B).
		currentTerm:   0,
		votedFor:      int32(-1), // -1 indicates that no vote has been cast in the current term
		log:           make([]LogEntry, 1),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		state:         Follower,
		heartbeatCh:   make(chan AppendEntriesArgs, 100),
		clientCh:      make(chan struct{}, 1),
		electionTimer: time.NewTimer(time.Duration(ElectionTimeoutMin+rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)) * time.Millisecond),
		replicating:   false,
	}

	// initialize nextIndex for each peer
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1 // Initial log index is 1 after the dummy entry at index 0
		rf.matchIndex[i] = 0
	}

	// Initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// Start a goroutine that listens on applyCh to apply committed log entries
	// go rf.applyLog()
	// TOCHECK
	// ^^maybe useful for task B^^

	// Now reset the election timer properly to start with a randomized timeout
	rf.resetElectionTimer()

	return rf
}

// Utility function to get the last log index and term
func (rf *Raft) getLastLogIndexAndTerm() (int32, int32) {
	rf.mu.Lock()
	lastIndex := int32(len(rf.log) - 1)
	if atomic.LoadInt32(&lastIndex) >= 0 {
		temp := int32(rf.log[atomic.LoadInt32(&lastIndex)].Term)
		rf.mu.Unlock()
		return atomic.LoadInt32(&lastIndex), temp
	}
	rf.mu.Unlock()
	return 0, 0 // Return default values if log is empty
}

func (rf *Raft) sendHeartbeats() {
	if atomic.LoadInt32(&rf.state) != Leader {
		return // Ignore the reply if we are no longer the leader or if the term has changed.
	}

	// var countReplies int32 = 0
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	for i := range rf.peers { // For all nodes
		if i != rf.me { // Except myself
			// rf.mu.Lock()
			// if rf.nextIndex[i] < rf.nextIndex[rf.me] {
			// 	go rf.replicateLogs(rf.commitIndex)
			// }
			// rf.mu.Unlock()

			go func(server int) { // Send a rountine
				args := AppendEntriesArgs{ // Which is an AppendEntries() (which is a heartbeat)
					Term:         atomic.LoadInt32(&rf.currentTerm),
					LeaderId:     rf.me,
					PrevLogIndex: atomic.LoadInt32(&lastLogIndex),
					PrevLogTerm:  atomic.LoadInt32(&lastLogTerm),
					Entries:      nil, // For heartbeat, the Entries slice is empty.
					LeaderCommit: atomic.LoadInt32(&rf.commitIndex),
				}
				var reply AppendEntriesReply
				if rf.sendAppendEntries(server, &args, &reply) {
					select {
					case rf.clientCh <- struct{}{}:
					default:
					}
				}
				// if !reply.Success {
				// 	atomic.AddInt32(&countReplies, 1)
				// }
			}(i)
		}
	}

	// if(countReplies == len(rf.peers)-1){
	// 	atomic.StoreInt32(&rf.state, Follower)
	// }
}

func (rf *Raft) convertToLeader() {
	if atomic.LoadInt32(&rf.state) != Candidate {
		return // Only a candidate can become a leader
	}

	atomic.StoreInt32(&rf.state, Leader)
	//log.Printf("Raft %d: State changed to Leader", rf.me)
	lastIndex, _ := rf.getLastLogIndexAndTerm() // Term is not needed for this operation
	for i := range rf.peers {
		if i != rf.me {
			rf.mu.Lock()
			rf.nextIndex[i] = int(atomic.LoadInt32(&lastIndex)) + 1 // Increment index of each follower to match last in leader log
			rf.matchIndex[i] = 0                                    // No entries known to be replicated yet
			rf.mu.Unlock()
		}
	}

	// Send initial heartbeats immediately to assert leadership
	// fmt.Printf("Attempting to send heartbeat.\n")
	rf.sendHeartbeats() // add go
}
