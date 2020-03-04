package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"

	"g.csail.mit.edu/6.824/src/labrpc"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct{}

const (
	follower  = iota
	candidate = iota
	leader    = iota
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []Log
	role        int
	receivedHR  bool

	votesReceived int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.role == leader
	return
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateID int // candidate requesting vote
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("Node %v receive RequestVote RPC, args %v\n", rf.me, args)
	rf.mu.Lock()
	rf.receivedHR = true
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	// RequestVote RPC:
	// 1. reply false if term < currentTerm
	// 2. If voteFor is null cor candidateId, grant vote
	reply.Term = currentTerm
	if args.Term < currentTerm {
		reply.VoteGranted = false
		DPrintf("Node %v VoteGranted = %v for candidate %v of term %v\n", rf.me, reply.VoteGranted, args.CandidateID, args.Term)
		return
	}

	if args.Term > currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.role = follower
		rf.mu.Unlock()
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		DPrintf("Node %v VoteGranted = %v, votedFor = %v\n", rf.me, reply.VoteGranted, args.CandidateID)
		return
	}

	reply.VoteGranted = false
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		DPrintf("Node %v timeout to receive reply from server %v\n", rf.me, server)
		return false
	}

	DPrintf("Node %v receive reply from server %v, reply is %v\n", rf.me, server, reply)

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	replyTerm := reply.Term
	rf.mu.Unlock()

	if replyTerm > currentTerm {
		rf.mu.Lock()
		rf.currentTerm = reply.Term
		rf.role = follower
		rf.mu.Unlock()

		DPrintf("Node %v back to follower, currentTerm: %v\n", rf.me, rf.currentTerm)
	} else {
		if reply.VoteGranted {
			rf.votesReceived++
			if rf.role != leader && rf.votesReceived > len(rf.peers)/2.0 {
				DPrintf("Node %v become leader\n", rf.me)
				rf.mu.Lock()
				rf.role = leader
				rf.votesReceived = 0
				rf.mu.Unlock()
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderID: rf.me,
				}
				var reply AppendEntriesReply
				for i := range rf.peers {
					if i != rf.me {
						rf.sendAppendEntries(i, &args, &reply)
					}
				}
				DPrintf("Leader %d send HR, args: %v\n", rf.me, args)
			}
		}
	}

	return ok
}

// Invoked by leader to replicate log entries; also used as heartbeat
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderID     int   // so follower can redirect clients
	prevLogIndex int   // index of log entries immediately preceding new ones
	prevLogTerm  int   // term of prevLogIndex entry
	entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	leaderCommit int   // leader's commitIndex
}

//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Node %d receive AppendEntries\n", rf.me)
	rf.mu.Lock()
	rf.receivedHR = true
	rf.mu.Unlock()

	// # AppendEntries RPC part
	// 1. update reply.Term to currentTerm, for leader to update itself
	// 2. reply false if term < currentTerm
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		reply.Success = true
	}

	// # Rules for Servers part
	// 1. All Servers:
	// if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		if rf.role == follower {
			DPrintf("Follower %d is already follower\n", rf.me)
		} else if rf.role == candidate {
			DPrintf("Candidate %d become follower\n", rf.me)
		} else if rf.role == leader {
			DPrintf("Leader %d become follower\n", rf.me)
		}
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.role = follower
		rf.votedFor = -1
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = follower
		}
		rf.mu.Unlock()
	}

	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("Node %v is killed", rf.me)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = follower

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {

			// # Rules for Servers
			// Followers:
			// 	If election timeout elapses without receiving AppendEntries RPC from current leader
			//	or granting vote to candidate: convert to candidate
			var role int

			rf.mu.Lock()
			role = rf.role
			rf.mu.Unlock()

			if role == follower {
				DPrintf("Node %v role is follower", rf.me)
			} else if role == candidate {
				DPrintf("Node %v role is candidate", rf.me)
			} else {
				DPrintf("Node %v role is leader", rf.me)
			}

			if role == follower {
				rf.mu.Lock()
				receivedHR := rf.receivedHR
				if !receivedHR {
					DPrintf("Node %v didn't receive HR, become candidate and start election\n", rf.me)
					rf.role = candidate
				}
				rf.receivedHR = false
				rf.mu.Unlock()
			}

			// # Rules for Servers
			// Candidates:
			//	On conversion to candidate, start election:
			//		Increment currentTerm
			//		Vote for self
			//		Reset election timer
			//		Send RequestVote RPCs to all other servers
			rf.mu.Lock()
			role = rf.role
			rf.mu.Unlock()
			if role == candidate {
				rf.mu.Lock()
				rf.currentTerm++
				rf.mu.Unlock()
				DPrintf("Candidate %v currentTerm is %v\n", rf.me, rf.currentTerm)
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateID: rf.me,
				}
				var reply RequestVoteReply
				DPrintf("Candidate %v start election, send request votes, args: %v\n", rf.me, args)
				for i := range rf.peers {
					if i != rf.me {
						go func(j int) {
							rf.sendRequestVote(j, &args, &reply)
						}(i)
					}
				}
			}

			// Leaders
			rf.mu.Lock()
			role = rf.role
			rf.mu.Unlock()
			if role == leader {
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderID: rf.me,
				}
				var reply AppendEntriesReply
				for i := range rf.peers {
					if i != rf.me {
						rf.sendAppendEntries(i, &args, &reply)
					}
				}
				DPrintf("Leader %d send HR, args: %v\n", rf.me, args)
			}

			if role == leader {
				// Leader heartbeat timeout
				duration := (time.Duration)(rand.Float64()*50) + 100
				DPrintf("Node %v sleep %d ms\n", rf.me, duration)
				time.Sleep(duration * time.Millisecond)
			} else {
				// follower heartbeat timeout
				duration := (time.Duration)(rand.Float64()*200) + 150
				DPrintf("Node %v sleep %d ms\n", rf.me, duration)
				time.Sleep(duration * time.Millisecond)
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
