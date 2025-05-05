package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	tester "6.5840/tester1"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
)

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

const (
	ElectionTimeout  = 300 * time.Millisecond
	HeartbeatTimeout = 50 * time.Millisecond
)

const (
	MaxEntriesSize = 50
)

func sendSignal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func electionTimeoutRand(factor float32) time.Duration {
	timeout := ElectionTimeout * time.Duration(factor)
	return timeout + time.Duration(rand.Int63n(int64(timeout)))
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state        string
	eventChan    chan struct{}
	lastLogIndex int
	lastLogTerm  int

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Printf("Raft %d received RequestVote from Raft %d, when state %v currentTerm %v lastLogTerm %v lastLogIndex %v, args %v",
	//	rf.me, args.CandidateId, rf.state, rf.currentTerm, rf.lastLogTerm, rf.lastLogIndex, args)
	//defer log.Printf("Raft %d responded RequestVote, reply: %v", rf.me, reply)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 {
		reply.Term = args.Term
		reply.VoteGranted = rf.votedFor == args.CandidateId
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	if (rf.lastLogTerm < args.LastLogTerm) || (rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex <= args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		sendSignal(rf.eventChan)
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	sendSignal(rf.eventChan)
	return
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.state = Follower
	}

	if rf.lastLogIndex < args.PrevLogIndex || rf.lastLogTerm != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		sendSignal(rf.eventChan)
		return
	}

	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	rf.lastLogIndex = len(rf.log) - 1
	rf.lastLogTerm = rf.log[rf.lastLogIndex].Term

	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, rf.lastLogIndex)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	sendSignal(rf.eventChan)
	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//log.Printf("rpc call RequestVote from %d to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, signal chan struct{}) {
	//log.Printf("rpc call AppendEntries from %d to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Printf("server %d send heartbeat to %d, get reply %v", rf.me, server, reply)

	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		sendSignal(rf.eventChan)
		return
	}

	if !reply.Success {
		rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
		sendSignal(signal)
		return
	}

	preMatchIndex := rf.matchIndex[server]

	rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
	rf.matchIndex[server] = rf.nextIndex[server] - 1

	if preMatchIndex < rf.matchIndex[server] && rf.matchIndex[server] > rf.commitIndex {
		// update commitIndex
		for index := rf.lastLogIndex; index > rf.commitIndex; index-- {

			if rf.log[index].Term != rf.currentTerm {
				break
			}

			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= index {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				rf.commitIndex = index
				break
			}
		}
	}

	if rf.matchIndex[server] < rf.lastLogIndex {
		sendSignal(signal)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})

		rf.lastLogIndex = len(rf.log) - 1
		rf.lastLogTerm = rf.currentTerm

		//log.Printf("Raft %d start command %v, log %v", rf.me, command, rf.lastLogTerm)

		//rf.persist()
		return rf.lastLogIndex, rf.currentTerm, true
	} else {
		return -1, rf.currentTerm, false
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		//log.Printf("server %d state %s term %d", rf.me, rf.state, rf.currentTerm)

		switch rf.state {
		case Follower:
			timer := time.NewTimer(electionTimeoutRand(1))
			rf.mu.Unlock()

			select {
			case <-rf.eventChan:
				rf.mu.Lock()
			case <-timer.C:
				rf.mu.Lock()
				rf.state = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
			}
		case Candidate:
			timer := time.NewTimer(electionTimeoutRand(1.2))
			go rf.startElection(rf.currentTerm)
			rf.mu.Unlock()

			select {
			case <-rf.eventChan:
				rf.mu.Lock()
			case <-timer.C:
				rf.mu.Lock()
				rf.currentTerm++
			}
		case Leader:
			rf.startHeartbeat()
			rf.mu.Unlock()
			<-rf.eventChan
			rf.mu.Lock()
		}
	}
}

func (rf *Raft) startElection(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.lastLogTerm,
	}

	votes := 1
	voteServers := make(map[int]struct{})
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		voteServers[i] = struct{}{}
	}

	for !rf.killed() && rf.state == Candidate && rf.currentTerm == args.Term {
		for server := range voteServers {
			go func(server int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						sendSignal(rf.eventChan)
						return
					}

					if reply.Term != rf.currentTerm || rf.state != Candidate {
						return
					}

					if _, voted := voteServers[server]; !voted {
						return
					}

					delete(voteServers, server)
					if reply.VoteGranted {
						votes++
						if votes > len(rf.peers)/2 {
							rf.state = Leader
							rf.votedFor = rf.me
							sendSignal(rf.eventChan)

							//log.Printf("server %d become leader term %d, votes %v", rf.me, rf.currentTerm, votedServer)
							return
						}
					}
				}
			}(server)
		}

		rf.mu.Unlock()
		time.Sleep(HeartbeatTimeout)
		rf.mu.Lock()
	}
	return
}

func (rf *Raft) startHeartbeat() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendHeartbeat(i)
	}
}

func (rf *Raft) sendHeartbeat(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() && rf.state == Leader {
		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := rf.log[prevLogIndex].Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      make([]LogEntry, 0),
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}

		if prevLogIndex < rf.lastLogIndex {
			endLogIndex := min(prevLogIndex+MaxEntriesSize, rf.lastLogIndex)
			args.Entries = rf.log[prevLogIndex+1 : endLogIndex+1]
		}

		//log.Println("server", rf.me, "send heartbeat to", i, "args", args)
		timer := time.NewTimer(HeartbeatTimeout)
		rf.mu.Unlock()

		logSignal := make(chan struct{})
		go rf.sendAppendEntries(i, args, reply, logSignal)

		select {
		case <-logSignal:
		case <-timer.C:
		}
		rf.mu.Lock()
	}

	//log.Printf("heart beat to %d end, server %d", i, rf.me)

	sendSignal(rf.eventChan)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.eventChan = make(chan struct{}, 1)
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{
		Term:    0,
		Command: struct{}{},
	}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start apply goroutine
	go rf.startApply(applyCh)

	return rf
}

func (rf *Raft) startApply(applyCh chan raftapi.ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied || rf.killed() {
			if rf.killed() {
				close(applyCh)
				return
			}

			applyMessages := make([]raftapi.ApplyMsg, rf.commitIndex-rf.lastApplied)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMessages[i-rf.lastApplied-1] = raftapi.ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.log[i].Command,

					SnapshotValid: false,
				}
			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()

			for _, applyMsg := range applyMessages {
				applyCh <- applyMsg
			}
		} else {
			rf.mu.Unlock()
		}
	}
}
