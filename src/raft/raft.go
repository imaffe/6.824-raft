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
	"runtime"
	"strconv"
	"strings"

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

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
	// newly added topics for me
	dRole      logTopic = "ROLE"
	dFollower  logTopic = "FOLW"
	dCandidate logTopic = "CAND"
	dLock      logTopic = "LOCK"
)

const (
	ElectionTimeoutLowerMs int64 = 500
	ElectionTimeoutUpperMs int64 = 900

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

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}

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
	logEntries  []LogEntry

	// volatile: all servers
	commitIndex int
	lastApplied int

	// volatile: leader only
	nextIndex  []int
	matchIndex []int

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

	applyCh chan ApplyMsg
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
	LastLogIndex int
	LastLogTerm int
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
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()
	// When fallback to follower, we reset voteFor and election timeout
	peerTerm := args.RequestTerm
	if peerTerm > rf.currentTerm {
		// when the peerTerm is higher than currentTerm in a RequestVote RPC, means the peer is in candidate state.
		// fall back to follower state
		Debug(dRole, "S%d <- S%d, saw higher term in RequestVote, currentTerm: %d, peerTerm: %d", rf.me, args.CandidateId, rf.currentTerm, peerTerm)
		rf.currentTerm = peerTerm
		rf.role = Follower
		// TODO when converting back to follower, should we reset the Timer ? should we reset the votedFor ? Should we reject the response ?
		// we need to reset votedFor because votedFor is the vote in currentTerm, meaning if term updates, the votedFor will be updated.
		rf.votedFor = -1
		rf.resetElectionTimer()
	}

	// if peerTerm <= requestTerm, and we could be in Follower / Candidate / Leader three state.

	currentTerm := rf.currentTerm
	votedFor := rf.votedFor

	// The peer will become follower
	if args.RequestTerm < currentTerm {
		reply.ReplyTerm = currentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d <- S%d, Request Vote rejected peer term low, currentTerm: %d, peerTerm: %d", rf.me, args.CandidateId, currentTerm, peerTerm)
		return
	}

	// if not voted, then vote, this does not involve candidateId
	if votedFor == -1 {
		// TODO now always return true, don't check the log

		if rf.isAsUpToDate(args) {
			reply.ReplyTerm = currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			// TODO seems we don't need to reset the timer here, but doesn't matter
			rf.resetElectionTimer()
			Debug(dVote, "S%d <- S%d, Request Vote Granted, currentTerm: %d, peerTerm: %d", rf.me, args.CandidateId, currentTerm, peerTerm)
			return
		} else {
			reply.ReplyTerm = currentTerm
			reply.VoteGranted = false
			Debug(dVote, "S%d <- S%d, Request Vote rejected, already voted, currentTerm: %d, peerTerm: %d", rf.me, args.CandidateId, currentTerm, peerTerm)
			return
		}
	} else {
		reply.ReplyTerm = currentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d <- S%d, Request Vote rejected, already voted, currentTerm: %d, peerTerm: %d", rf.me, args.CandidateId, currentTerm, peerTerm)
		return
	}
}

func (rf *Raft) isAsUpToDate(args *RequestVoteArgs) bool {
	if args.LastLogTerm > rf.logEntries[len(rf.logEntries) - 1].Term {
		return true
	} else if args.LastLogTerm < rf.logEntries[len(rf.logEntries) - 1].Term {
		return false
	} else {
		return args.LastLogIndex >= len(rf.logEntries) - 1
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
	if changed, currentTerm := rf.hasCurrentTermChangedDuringRpc(server, args.RequestTerm); changed {
		if args.RequestTerm != currentTerm {
			Debug(dInfo, "S%d -> S%d currentTerm has changed during RequestVote RPC, should be %d but now is %d", rf.me, server, args.RequestTerm, currentTerm)
		}
		return false
	}

	if !ok {
		Debug(dInfo, "S%d -> S%d RPC call failed in RequestVote request", rf.me, server)
	}
	return ok
}

func (rf *Raft) goSendRequestVoteAndHandle(server int, args *RequestVoteArgs, reply *RequestVoteReply, votesGranted *int64) {
	Debug(dVote, "S%d -> S%d send RequestVote to peer, candi term %d", rf.me, server, args.RequestTerm)
	validReply := rf.sendRequestVote(server, args, reply)
	if !validReply {
		// drop the reply if not valid or timeouted
		Debug(dVote, "S%d -> S%d received invalid RequestVote reply from peer: %d", rf.me, server, server)
		return
	}

	// if RequestVote found a higher term, fall back to follower, and should stop candidateProcess
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()
	peerTerm := reply.ReplyTerm
	// TODO TODO could currentTerm changed here ?
	// Check again. and continue processing
	if rf.currentTerm != args.RequestTerm {
		Debug(dError, "S%d -> S%d term has changed when processing RequestVoteRPC", rf.me, server, rf.currentTerm, peerTerm)
		return
	}
	if peerTerm > rf.currentTerm {
		// TODO should be currentTerm or args.RequestTerm ? since while waiting for reply we could see the term change
		Debug(dVote, "S%d -> S%d saw higher term in RequestVoteReply,term:%d,peerTerm:%d", rf.me, server, rf.currentTerm, peerTerm)
		// fall back to follower state
		rf.currentTerm = peerTerm
		rf.role = Follower
		rf.votedFor = -1
		rf.resetElectionTimer()
		return
	}

	if reply.VoteGranted {
		atomic.AddInt64(votesGranted, 1)
		Debug(dVote, "S%d -> S%d requestVote Granted, total: %d, for term: %d", rf.me, server, atomic.LoadInt64(votesGranted), args.RequestTerm)
	}
}

type AppendEntriesArgs struct {
	RequestTerm int
	LeaderId    int
	// The following are used in 2B
	PrevLogIndex int
	PrevLogTerm int
	LogEntries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	ReplyTerm int
	Success   bool
}

// TODO this is an RPC call, need to perform some checks
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()
	peerTerm := args.RequestTerm

	// TODO how to convert back to follower ? How to know the states ? Just stop any Candidate or Leader go routine ?
	// We could be receiving appendEntries as Follower / Candidate / Leader
	if peerTerm > rf.currentTerm {
		// fall back to follower state
		Debug(dInfo, "S%d <- S%d, saw higher term in AppendEntries, currentTerm: %d, peerTerm: %d", rf.me, args.LeaderId, rf.currentTerm, peerTerm)
		rf.currentTerm = peerTerm
		rf.role = Follower
		// TODO when converting back to follower, should we reset the Timer ? should we reset the votedFor ?
		rf.votedFor = -1
		rf.resetElectionTimer()
		// We need to make sure the leader process will be stopped
	}

	// continue to process the request
	currentTerm := rf.currentTerm
	if args.RequestTerm < currentTerm {
		Debug(dFollower, "S%d <- S%d, reject AppendEntries log, currentTerm: %d, peerTerm: %d", rf.me, args.LeaderId, rf.currentTerm, peerTerm)
		reply.ReplyTerm = currentTerm
		reply.Success = false
		return
	} else {

		// prevLogIndex points beyond current index
		if args.PrevLogIndex >= len(rf.logEntries) {
			reply.ReplyTerm = currentTerm
			reply.Success = false
			rf.resetElectionTimer()
			return
		}

		// prevLogI
		if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.ReplyTerm = currentTerm
			reply.Success = false
			rf.resetElectionTimer()
			return
		}
		// TODO what if prevLogIndex is 0 ? The term and index all matches, meaning they should accept the log

		// Then we should merge the new entries with the current entries
		Debug(dFollower, "S%d <- S%d, accepted AppendEntries, currentTerm: %d, peerTerm: %d", rf.me, args.LeaderId, rf.currentTerm, peerTerm)

		// TODO what if this is a heartbeat message ? Is this implementation safe ?
		newEntriesStartIndex := args.PrevLogIndex + 1

		// TODO might want to revisit this logic here
		for i := range args.LogEntries {
			// try append, and truncate if mismatch
			if newEntriesStartIndex + i < len(rf.logEntries) {
				if args.LogEntries[i].Term != rf.logEntries[newEntriesStartIndex + i].Term {
					// remove all after it
					rf.logEntries = rf.logEntries[:newEntriesStartIndex + i ]
					rf.logEntries = append(rf.logEntries, args.LogEntries[i:]...)
					break
				}
			} else {
				// stopped at the logEntries
				rf.logEntries = append(rf.logEntries, args.LogEntries[i:]...)
				break
			}
		}
		// 5. check commit index
		if args.LeaderCommit > rf.commitIndex {
			Debug(dCommit, "S%d <- S%d, received higher leader commit index %d, current %d, lastLog %d",rf.me, args.LeaderId, args.LeaderCommit, rf.commitIndex, len(rf.logEntries) - 1)
			rf.commitIndex = min(args.LeaderCommit, len(rf.logEntries) - 1)
		}


		reply.ReplyTerm = currentTerm
		reply.Success = true
		rf.resetElectionTimer()
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if changed, currentTerm := rf.hasCurrentTermChangedDuringRpc(server, args.RequestTerm); changed {
		if args.RequestTerm != currentTerm {
			Debug(dInfo, "S%d -> S%d currentTerm has changed during AppendEntries RPC, should be %d but now is %d, goId: %d", rf.me, server, args.RequestTerm, currentTerm, GoID())
		}
		return false
	}

	//if !ok {
	//	Debug(dError, "S%d -> S%d RPC call returned false in AppendEntries", rf.me, server)
	//}
	return ok
}

func (rf *Raft) goSendAppendEntriesAndHandle(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	validReply := rf.sendAppendEntries(server, args, reply)
	if !validReply {
		Debug(dLeader, "S%d -> S%d, received invalid heartbeat reply from %d", rf.me, server, server)
		// drop the reply if not valid or timeouted
		return
	}

	// if AppendEntries found a higher term, fall back to follower, and should stop candidateProcess
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()
	if rf.currentTerm != args.RequestTerm {
		Debug(dError, "S%d -> S%d term has changed when processing AppendEntries RPC, currentTerm: %d requesetTerm: %d", rf.me, server, rf.currentTerm, args.RequestTerm)
		return
	}

	peerTerm := reply.ReplyTerm
	if peerTerm > rf.currentTerm {
		Debug(dRole, "S%d -> S%d, saw higher term in AppendEntries Reply, currentTerm: %d, peerTerm: %d", rf.me, server, rf.currentTerm, peerTerm)
		// fall back to follower state
		rf.currentTerm = peerTerm
		rf.role = Follower
		rf.votedFor = -1
		rf.resetElectionTimer()
		return
	}

	// TODO the temporary new code area
	// 1. if the leader is no longer the leader, what to do ?
	// TODO need to think about what if some thing has changed?
	if reply.Success {
		// rf.nextIndex[peer] remain unchanged, TODO why remain unchanged ?
		// TODO will matchIndex decrease ?
		Debug(dLeader, "S%d <- S%d appendEntries accepted by peer, s", rf.me, server, )
		rf.matchIndex[server] = args.PrevLogIndex + len(args.LogEntries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		// need to decrease the nextIndex
		//rf.nextIndex[server] = rf.nextIndex[server] - 1
		// TODO which one is correct ?
		// TODO will it fall back ?
		rf.nextIndex[server] = (args.PrevLogIndex + 1) - 1
	}

	for index := len(rf.logEntries) - 1; index > rf.commitIndex; index -- {
		// if term is low, cannot commit any entries
		if rf.logEntries[index].Term != rf.currentTerm {
			break
		}
		// leader always count as 1
		replicated := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			// TODO will matchIndex be changed here ?
			if rf.matchIndex[peer] >= index {
				Debug(dCommit, "S%d <- S%d peer has match index larger than %d",rf.me, peer, index)
				replicated++
			}
		}
		if replicated > len(rf.peers) / 2 {
			Debug(dCommit, "S%d commit index %d, replicated %d",rf.me, index, replicated)
			rf.commitIndex = index
			break
		}
	}
}

func (rf *Raft) hasCurrentTermChangedDuringRpc(server int, termInRpcRequest int) (bool, int) {
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()
	return termInRpcRequest != rf.currentTerm, rf.currentTerm
}

// TODO should we compare with rf.currentTerm or the term included in the original RPC ?
//func (rf *Raft) checkTermAndFallBackToFollower(term int) bool {
//	rf.raftLock.Lock()
//	defer rf.raftLock.Unlock()
//
//	// TODO how to convert back to follower ? How to know the states ? Just stop any Candidate or Leader go routine ?
//	if term > rf.currentTerm {
//		// fall back to follower state
//		rf.currentTerm = term
//		rf.role = Follower
//		// TODO when converting back to follower, should we reset the Timer ? should we reset the votedFor ?
//		rf.votedFor = -1
//		rf.resetElectionTimer()
//		return true
//	} else {
//		return false
//	}
//}

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
	// Your code here (2B).
	// TODO possible dead lock here ?
	rf.raftLock.Lock()
	// Proceed as Leader take a snapshot of the data we will be using to start the process
	termWhenReceivedCommand := rf.currentTerm
	role := rf.role


	if role != Leader {
		rf.raftLock.Unlock()
		return -1, termWhenReceivedCommand, false
	}

	// 1. append to local entry
	newEntry := LogEntry{
		Term: termWhenReceivedCommand,
		Index: len(rf.logEntries),
		Command: command,
	}
	rf.logEntries = append(rf.logEntries, newEntry)
	// TODO this update is not necessary ?

	// 2. send AppendEntries to peers
	rf.goSendAppendEntriesAndHandleNew(termWhenReceivedCommand)
	rf.refreshLastSendAppendEntriesTime()
	// release lock before we enter wait status
	rf.raftLock.Unlock()

	// 3. wait for the results
	return newEntry.Index, newEntry.Term, true
}

func (rf *Raft) goSendAppendEntriesAndHandleNew(termWhenReceivedCommand int) {

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// prepare data, some needs to be update
		nextIndex := make([]int, len(rf.nextIndex))
		copy(nextIndex, rf.nextIndex)

		matchIndex := make([]int, len(rf.matchIndex))
		copy(matchIndex, rf.matchIndex)

		commitIndex := rf.commitIndex

		entries := make([]LogEntry, len(rf.logEntries[rf.nextIndex[peer]:]))
		copy(entries, rf.logEntries[rf.nextIndex[peer]:])


		// logEntries is guaranteed to exist at prevLogIndex
		prevLogIndex := rf.nextIndex[peer] - 1
		prevLogTerm := rf.logEntries[prevLogIndex].Term


		// let's do not retry for now

		request := AppendEntriesArgs{
			RequestTerm: termWhenReceivedCommand,
			LeaderId: rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm: prevLogTerm,
			LogEntries: entries,
			LeaderCommit: commitIndex,
		}

		reply := AppendEntriesReply{}

		go rf.goSendAppendEntriesAndHandle(peer, &request, &reply)
	}
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
	Debug(dTimer, "S%d Election Timer starts", rf.me)
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// TODO is the lock reentrant ?
		rf.raftLock.Lock()
		role := rf.role
		electionTimerStartTime := rf.electionTimerStartTime
		electionTimeout := rf.ElectionTimeout
		rf.raftLock.Unlock()
		// only pause election timeout ticker when in leader role
		if role == Leader {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		now := time.Now()
		if now.Sub(electionTimerStartTime).Milliseconds() >= electionTimeout {
			Debug(dTimer, "S%d election timer triggered, startTime: %d, currentTime: %d", rf.me, GetTimeSinceStart(electionTimerStartTime), GetTimeSinceStart(time.Now()))
			// TODO here the lock is not continuous, will it cause any trouble ?
			requests, replys, candidateTerm:= rf.candidatePrepareNewElection()
			go rf.goCandidateStartNewElection(requests, replys, candidateTerm)
		} else {
			time.Sleep(electionTimerStartTime.Add(time.Duration(electionTimeout) * time.Millisecond).Sub(now))
		}

	}
	Debug(dInfo, "S%d Killed, exit ticker", rf.me)
}

func (rf *Raft) applier() {
	for rf.killed() == false {


		var msgsToApply []ApplyMsg
		rf.raftLock.Lock()
		if rf.commitIndex > rf.lastApplied {
			msgsToApply = make([]ApplyMsg, rf.commitIndex - rf.lastApplied)
			for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
				msgsToApply[index- (rf.lastApplied + 1)] = ApplyMsg{
					CommandValid:  true,
					Command:       rf.logEntries[index].Command,
					CommandIndex:  rf.logEntries[index].Index,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
			}
		}
		rf.raftLock.Unlock()

		// TODO this method could block
		for i := range msgsToApply {
			rf.applyCh <- msgsToApply[i]
		}

		// set the sleep interval
		time.Sleep(100 * time.Millisecond)
	}
}

// reset the election timeout timer to time.Now()
func (rf *Raft) resetElectionTimer() {
	//rf.electionTimerStartTimeLock.Lock()
	//defer rf.electionTimerStartTimeLock.Unlock()
	newStartTime := time.Now()
	Debug(dTimer, "S%d reset Election Timer, newStartTime: %d", rf.me, GetTimeSinceStart(newStartTime))
	rf.electionTimerStartTime = newStartTime
	rf.ElectionTimeout = setRandomElectionTimeout()
	// TODO should we reset the voted for ? No
}

// Candidate Related Methods

func (rf *Raft) candidatePrepareNewElection() ([]RequestVoteArgs, []RequestVoteReply, int) {
	Debug(dCandidate, "S%d candidate start election", rf.me)
	Debug(dLock, "S%d acquiring raftLock", rf.me)
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()
	rf.resetElectionTimer()

	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.role = Candidate

	Debug(dRole, "S%d Role set to candidate", rf.me)

	thisElectionTerm := rf.currentTerm

	// composing requests and replies
	requests := make([]RequestVoteArgs, len(rf.peers))
	replys := make([]RequestVoteReply, len(rf.peers))
	Debug(dCandidate, "S%d candidate term: %d", rf.me, thisElectionTerm)
	for i := range requests {
		requests[i] = RequestVoteArgs{
			RequestTerm: thisElectionTerm,
			CandidateId: rf.me,
			LastLogIndex: len(rf.logEntries) - 1,
			LastLogTerm: rf.logEntries[len(rf.logEntries) - 1].Term,
		}
		replys[i] = RequestVoteReply{}
	}
	return requests, replys, rf.currentTerm
}
func (rf *Raft) goCandidateStartNewElection(requests []RequestVoteArgs, replys []RequestVoteReply, thisElectionTerm int) {
	Debug(dLock, "S%d released raftLock", rf.me)

	// TODO do we need to retry here ? Or it automatically retry for us ?
	var votesGranted int64 = 1
	for peer := range rf.peers {
		// send the request, if currentTerm has changed either because a new leader or a higher term candidate is present
		// we can make sure the term has changed since we sent out the request, and thus such replies will be dropped.
		if peer != rf.me {
			go rf.goSendRequestVoteAndHandle(peer, &requests[peer], &replys[peer], &votesGranted)
		}
	}

	// While waiting for election result, we should check if current node is still in candidate state
	for !rf.killed() && !rf.hasEnoughVotes(int(atomic.LoadInt64(&votesGranted))) {
		role, term := rf.getRoleAndTerm()
		// this follower could be a result of : appendEntries() or RequestVoteReply
		// we just exit the process and stop being election.
		// However, in terms of timeout, how do we exit the process
		if role == Follower {
			// TODO if we are follower, should we return early and forget about the requests ? ?
			Debug(dCandidate, "S%d became follower while waiting for votes", rf.me)
			return
		}
		// this means the new election has started, we should give up this election
		// TODO this is not working bro.
		if term != thisElectionTerm {
			Debug(dCandidate, "S%d candi's term changed while waiting for votes, should be %d but is %d", rf.me, thisElectionTerm, term)
			return
		}

		time.Sleep(time.Duration(50) * time.Millisecond)
	}

	// TODO how to fall back to follower in the middle of the process ?
	// only success when enough votes are granted
	rf.raftLock.Lock()
	if rf.hasEnoughVotes(int(atomic.LoadInt64(&votesGranted))) && rf.role == Candidate {
		Debug(dCandidate, "S%d got enough votes, will become leader", rf.me)
		rf.role = Leader
		for peer := range rf.nextIndex {
			rf.nextIndex[peer] = len(rf.logEntries)
			rf.matchIndex[peer] = 0
		}
		Debug(dRole, "S%d Role set to Leader", rf.me)
		go rf.goLeaderStartNewTerm(rf.currentTerm)

	}
	rf.raftLock.Unlock()
}

func (rf *Raft) hasEnoughVotes(votesGranted int) bool {
	return votesGranted > len(rf.peers)/2
}

func (rf *Raft) goLeaderStartNewTerm(termOfCandidate int) {
	role, currentTerm := rf.getRoleAndTerm()
	//initialize the time
	rf.setZeroLastSendAppendEntriesTime()
	for !rf.killed() && role == Leader {
		// Debug(dLeader, "S%d Leader entering heart beat loop, currentTerm:%d candidateTerm:%d", rf.me, currentTerm, termOfCandidate)
		if currentTerm != termOfCandidate {

			Debug(dInfo, "S%d is leader but term does not match, %d %d", rf.me, currentTerm, termOfCandidate)
			// need to break the loop if term no longer matches
			return
		}

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		lastAppendEntriesTime := rf.getLastSendAppendEntriesTime()

		// TODO needs to be updated as well.
		if lastAppendEntriesTime.IsZero() || time.Now().Sub(lastAppendEntriesTime).Milliseconds() >= HeartBeatIntervalMs {
			// TODO do we go here ? wee do not need to go here because we can fire and forget
			//rf.goLeaderSendHeartBeats(termOfCandidate)
			rf.raftLock.Lock()
			rf.goSendAppendEntriesAndHandleNew(termOfCandidate)
			rf.raftLock.Unlock()
			rf.refreshLastSendAppendEntriesTime()
		} else {
			time.Sleep(lastAppendEntriesTime.Add(time.Duration(HeartBeatIntervalMs) * time.Millisecond).Sub(time.Now()))
		}

		// check if role and term has changed
		role, currentTerm = rf.getRoleAndTerm()
	}
}

//func (rf *Raft) goLeaderSendHeartBeats(termOfCandidate int) {
//	// take a snapshot in case the term change suddenly
//	Debug(dLeader, "S%d Leader Start sending heartBeat,candidateTerm:%d", rf.me, termOfCandidate)
//	requests := make([]AppendEntriesArgs, len(rf.peers))
//	replys := make([]AppendEntriesReply, len(rf.peers))
//
//	for i := range requests {
//		requests[i] = AppendEntriesArgs{
//			RequestTerm: termOfCandidate,
//			LeaderId:    rf.me,
//		}
//		replys[i] = AppendEntriesReply{}
//	}
//
//	for peer := range rf.peers {
//		if peer == rf.me {
//			continue
//		}
//		// TODO this needs to be changed as well
//		go rf.goSendAppendEntriesAndHandle(peer, &requests[peer], &replys[peer])
//	}
//	// do not need to wait for results
//}

func (rf *Raft) getLastSendAppendEntriesTime() time.Time {
	rf.lastSendAppendEntriesTimeLock.Lock()
	defer rf.lastSendAppendEntriesTimeLock.Unlock()
	return rf.lastSendAppendEntriesTime
}

func (rf *Raft) refreshLastSendAppendEntriesTime() {
	rf.lastSendAppendEntriesTimeLock.Lock()
	defer rf.lastSendAppendEntriesTimeLock.Unlock()
	rf.lastSendAppendEntriesTime = time.Now()
}

func (rf *Raft) setZeroLastSendAppendEntriesTime() {
	rf.lastSendAppendEntriesTimeLock.Lock()
	defer rf.lastSendAppendEntriesTimeLock.Unlock()
	var zeroTime time.Time
	rf.lastSendAppendEntriesTime = zeroTime
}

func (rf *Raft) getRoleAndTerm() (Role, int) {
	rf.raftLock.Lock()
	defer rf.raftLock.Unlock()
	role := rf.role
	term := rf.currentTerm
	return role, term
}

func (rf *Raft) setRole(role Role) {
	//rf.raftLock.Lock()
	//defer rf.raftLock.Unlock()

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
	rf.applyCh = applyCh

	Init()
	// Your initialization code here (2A, 2B, 2C).

	initializeRaftAsFollower(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

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
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// Initialize the logEntries to have a length 1
	rf.logEntries = make([]LogEntry, 1)
	rf.logEntries[0] = LogEntry{}
	// other vars
	rf.electionTimerStartTime = time.Now()
	rf.ElectionTimeout = setRandomElectionTimeout()

	Debug(dInfo, "S%d Initialized as follower", rf.me)
	Debug(dInfo, "S%d Election Timeout set to %d", rf.me, rf.ElectionTimeout)
	// TODO lastHeartbeatStartTime should be zero value
}

func setRandomElectionTimeout() int64 {
	electionTimeoutInMs := rand.Int63n(ElectionTimeoutUpperMs-ElectionTimeoutLowerMs) + ElectionTimeoutLowerMs
	return electionTimeoutInMs
}

// TODO this can be deleted
func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(err)
	}
	return id
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}