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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

const (
	ElectionTimeoutLowerMs int64 = 400
	ElectionTimeoutUpperMs int64 = 800

	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2

	HeartBeatIntervalMs int64 = 120
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persist: all servers
	currentTerm int
	votedFor    int

	// volatile: all servers
	commitIndex int64
	lastApplied int64

	// volatile: leader only
	nextIndex  []int64
	matchIndex []int64

	// electionTimer vars and it's locks
	electionTimerStartTime     time.Time
	electionTimerStartTimeLock sync.Mutex

	ElectionTimeout int64

	raftLock sync.Mutex

	role Role

	// one goroutine only vars, no need to be protected
	lastSendAppendEntriesTime time.Time
	// TODO this lock is not used
	lastSendAppendEntriesTimeLock sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool
	// Your code here (2A).

	role, term := rf.getRoleAndTerm()
	isleader = role == Leader
	return term, isleader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	RequestTerm int
	CandidateId int
	//lastLogIndex int64
	//lastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	ReplyTerm   int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// TODO if fallback to follower, should we continue ?
	// This checkTermAndFallBackToFollower either happens before become candidate, or after become candidate.
	// 1. if after become candidate, easy, just fall back to follower and start election
	// 2. if before become candidate, then the term and votedFor could be changed after start election.
	rf.checkTermAndFallBackToFollower(args.RequestTerm)
	// TODO There is a timing issue, when fall back to follower, but godidate just starts, then the convert back is not working.
	// TODO also, what if the the receiver is candidate
	// always take a snapshot
	rf.raftLock.Lock()
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	rf.raftLock.Unlock()

	if args.RequestTerm < currentTerm {
		reply.ReplyTerm = currentTerm
		reply.VoteGranted = false
		return
	}

	if votedFor == -1 || votedFor == args.CandidateId {
		// TODO now always return true, don't check the log
		reply.ReplyTerm = currentTerm
		reply.VoteGranted = true
		rf.resetElectionTimer()
		return
	} else {
		reply.ReplyTerm = currentTerm
		reply.VoteGranted = false
		return
	}
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
	if rf.hasCurrentTermChangedDuringRpc(args.RequestTerm) {
		return false
	}
	return ok
}

func (rf *Raft) goSendRequestVoteAndHandle(server int, args *RequestVoteArgs, reply *RequestVoteReply, votesGranted *int64) {
	validReply := rf.sendRequestVote(server, args, reply)
	if !validReply {
		// drop the reply if not valid or timeouted
		return
	}

	if rf.checkTermAndFallBackToFollower(reply.ReplyTerm) {
		return
	}

	if reply.VoteGranted {
		atomic.AddInt64(votesGranted, 1)
	}
}


type AppendEntriesArgs struct {
	RequestTerm int
	LeaderId    int
	// The remaining won't be used now
}

type AppendEntriesReply struct {
	ReplyTerm int
	Success   bool
}

// TODO this is an RPC call, need to perform some checks
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.checkTermAndFallBackToFollower(args.RequestTerm)

	rf.raftLock.Lock()
	currentTerm := rf.currentTerm
	rf.raftLock.Unlock()

	if args.RequestTerm < currentTerm {
		reply.ReplyTerm = currentTerm
		reply.Success = false
	} else {
		reply.ReplyTerm = currentTerm
		reply.Success = true
		rf.resetElectionTimer()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if rf.hasCurrentTermChangedDuringRpc(args.RequestTerm) {
		return false
	}
	return ok
}

func (rf *Raft) hasCurrentTermChangedDuringRpc(termInRpcRequest int) bool {
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()

	return termInRpcRequest != rf.currentTerm
}

// TODO should we compare with rf.currentTerm or the term included in the original RPC ?
func (rf *Raft) checkTermAndFallBackToFollower(term int) bool {
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()

	// TODO how to convert back to follower ? How to know the states ? Just stop any Candidate or Leader go routine ?
	if term > rf.currentTerm {
		// fall back to follower state
		rf.currentTerm = term
		rf.role = Follower
		// TODO when converting back to follower, should we reset the Timer ? should we reset the votedFor ?
		rf.votedFor = -1
		rf.resetElectionTimer()
		return true
	} else {
		return false
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// TODO is the lock reentrant ?
		rf.electionTimerStartTimeLock.Lock()
		electionTimerStartTime := rf.electionTimerStartTime
		rf.electionTimerStartTimeLock.Unlock()

		if rf.checkShouldStartElection(electionTimerStartTime) {
			rf.resetElectionTimer()
			go rf.goCandidateStartNewElection()
		} else {
			rf.sleepUntilNextPossibleElectionTimeout(electionTimerStartTime)
		}
	}
}

// only start election when in follower mode
func (rf *Raft) checkShouldStartElection(electionTimerStartTime time.Time) bool {
	role, _:= rf.getRoleAndTerm()
	return role == Follower && time.Now().Sub(electionTimerStartTime).Milliseconds() >= rf.ElectionTimeout
}

func (rf *Raft) sleepUntilNextPossibleElectionTimeout(electionTimerStartTime time.Time) {
	time.Sleep(electionTimerStartTime.Add(time.Duration(rf.ElectionTimeout) * time.Millisecond).Sub(time.Now()))
}

// reset the election timeout timer to time.Now()
func (rf *Raft) resetElectionTimer() {
	rf.electionTimerStartTimeLock.Lock()
	defer rf.electionTimerStartTimeLock.Unlock()
	rf.electionTimerStartTime = time.Now()
	// TODO should we reset the voted for ? No
}

// Candidate Related Methods
func (rf *Raft) goCandidateStartNewElection() {

	rf.raftLock.Lock()

	rf.currentTerm = rf.currentTerm + 1
	if rf.votedFor != -1 {
		// TODO assert votedFor is set properly, when to reset the votedFor
	}
	rf.votedFor = rf.me
	rf.role = Candidate

	rf.raftLock.Unlock()

	rf.beginElection()
}

func (rf *Raft) beginElection() {

	requests := make([]RequestVoteArgs, len(rf.peers))
	replys := make([]RequestVoteReply, len(rf.peers))

	rf.raftLock.Lock()
	currentTerm := rf.currentTerm
	rf.raftLock.Unlock()

	for i := range requests {
		requests[i] = RequestVoteArgs{
			RequestTerm: currentTerm,
			CandidateId: rf.me,
		}

		replys[i] = RequestVoteReply{}
	}


	// TODO do we need to retry here ? Or it automatically retry for us ?
	// TODO when we have more than half of majority, do we need to wait for the rest peer to return ?
	// yes, because we need to step down from candidate and become follower again
	var votesGranted int64 = 1
	for peer := range rf.peers {
		// TODO what is the exit condition ? not candidate or is follower ?
		role, _ := rf.getRoleAndTerm()
		if role != Candidate || rf.killed() {
			break
		}

		if peer != rf.me {
			go rf.goSendRequestVoteAndHandle(peer, &requests[peer], &replys[peer], &votesGranted)
		}
	}

	for !rf.killed() && !rf.isElectedLeader(int(atomic.LoadInt64(&votesGranted))) {

		// TODO should we lock around role ?
		role, _ := rf.getRoleAndTerm()
		if role == Follower {
			// TODO if we are follower, should we return early and forget about the requests ? ?
			return
		}

		time.Sleep(time.Duration(50) * time.Millisecond)
	}

	// TODO how to fall back to follower in the middle of the process ?
	if rf.isElectedLeader(int(atomic.LoadInt64(&votesGranted))) {
		go rf.goLeaderStartNewTerm()
	}
}

func (rf *Raft) isElectedLeader(votesGranted int) bool {
	return votesGranted > len(rf.peers)/2
}

func (rf *Raft) goLeaderStartNewTerm() {
	rf.setRole(Leader)
	go rf.goLeaderStartHeartBeatLoop()
}

func (rf *Raft) goLeaderStartHeartBeatLoop() {
	role, _ := rf.getRoleAndTerm()
	for !rf.killed() && role == Leader {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.lastSendAppendEntriesTimeLock.Lock()
		lastAppendEntriesTime := rf.lastSendAppendEntriesTime
		rf.lastSendAppendEntriesTimeLock.Unlock()
		// TODO do we need to lock ? Since only the leader
		if lastAppendEntriesTime.IsZero() || time.Now().Sub(lastAppendEntriesTime).Milliseconds() >= HeartBeatIntervalMs {
			// TODO should we wait for the sendAppend to finish ?
			go rf.goLeaderSendHeartBeats()
			rf.setLastSendAppendEntriesTime()
		} else {
			time.Sleep(lastAppendEntriesTime.Add(time.Duration(HeartBeatIntervalMs) * time.Millisecond).Sub(time.Now()))
		}

		role,_  = rf.getRoleAndTerm()
	}
}

func (rf *Raft) goLeaderSendHeartBeats() {
	// take a snapshot in case the term change suddenly
	rf.raftLock.Lock()
	currentTerm := rf.currentTerm
	rf.raftLock.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		request := AppendEntriesArgs{
			RequestTerm: currentTerm,
			LeaderId: rf.me,
		}

		reply := AppendEntriesReply{}

		// TODO this is not parallelized right ?
		go rf.sendAppendEntries(peer, &request, &reply)
	}
}

func (rf *Raft) setLastSendAppendEntriesTime() {
	rf.lastSendAppendEntriesTimeLock.Lock()
	defer rf.lastSendAppendEntriesTimeLock.Unlock()
	rf.lastSendAppendEntriesTime = time.Now()
}

func (rf *Raft) getRoleAndTerm() (Role, int) {
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()
	role := rf.role
	term := rf.currentTerm
	return role, term
}

func (rf *Raft) setRole(role Role) {
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()
	rf.role = role
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

	// Your initialization code here (2A, 2B, 2C).

	initializeRaftAsFollower(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func initializeRaftAsFollower(rf *Raft) {

	// persist field
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	// volatile field
	rf.commitIndex = 0
	rf.lastApplied = 0
	// TODO volatile leader field, not used in 2A yet.
	rf.nextIndex = make([]int64, len(rf.peers))
	rf.matchIndex = make([]int64, len(rf.peers))

	// other vars
	rf.electionTimerStartTime = time.Now()
	rf.ElectionTimeout = setRandomElectionTimeout()

	// TODO lastHeartbeatStartTime should be zero value
}

func setRandomElectionTimeout() int64 {
	electionTimeoutInMs := rand.Int63n(ElectionTimeoutUpperMs-ElectionTimeoutLowerMs) + ElectionTimeoutLowerMs
	return electionTimeoutInMs
}
