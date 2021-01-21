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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//import "fmt"

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

const (
	Follower           = 1
	Candidate          = 2
	Leader             = 3
	HEART_BEAT_TIMEOUT = 100
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	CommandBytes []byte
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Snap struct {
	Term      int
	LeaderId  int
	DataBase  []byte
	LastIndex int
	LastTerm  int
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

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electTime   *time.Timer
	heartTime   *time.Timer
	state       int
	voteCounter int
	applyCh     chan ApplyMsg

	snapIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	isleader = false
	if rf.state == Leader {
		isleader = true
	}
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) codeRaft() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.snapIndex)
	return w.Bytes()
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	/*
	   w := new(bytes.Buffer)
	   e := labgob.NewEncoder(w)
	   e.Encode(rf.votedFor)
	   e.Encode(rf.log)
	   e.Encode(rf.currentTerm)
	   data := w.Bytes()
	   rf.persister.SaveRaftState(data)
	*/
	rf.persister.SaveRaftState(rf.codeRaft())
}

func (rf *Raft) GetDataSize() int {
	return rf.persister.RaftStateSize()
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor int
	var log []LogEntry
	var currentTerm int
	var snapIndex int
	if d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&currentTerm) != nil || d.Decode(&snapIndex) != nil {

		panic("decode error")

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapIndex = snapIndex
		rf.commitIndex = rf.snapIndex
		rf.lastApplied = rf.snapIndex
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}

	if args.LastLogTerm < rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < (len(rf.log)-1+rf.snapIndex)) {
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId

	var tm time.Duration = time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
	rf.electTime.Reset(tm)
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
	return ok
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
	rf.mu.Lock()
	isLeader = false
	term = rf.currentTerm
	if rf.state == Leader {
		isLeader = true
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		rf.persist()
		index = (len(rf.log) - 1 + rf.snapIndex)
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.heartBeat()
	}
	rf.mu.Unlock()

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

	rf.heartTime = time.NewTimer(HEART_BEAT_TIMEOUT * time.Millisecond)
	var tm time.Duration = time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
	rf.electTime = time.NewTimer(tm)

	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 1)
	rf.applyCh = applyCh

	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))

	go func() {
		for {
			select {
			case <-rf.heartTime.C:
				if rf.state == Leader {
					rf.mu.Lock()
					rf.heartBeat()
					rf.heartTime.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
					rf.mu.Unlock()
				}
			case <-rf.electTime.C:
				if rf.state == Follower {
					rf.mu.Lock()
					rf.switchStateTo(Candidate)
					rf.mu.Unlock()
				} else if rf.state == Candidate {
					rf.mu.Lock()
					rf.electionStarter()
					rf.mu.Unlock()
				}

			}
		}
	}()

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) electionStarter() {

	rf.votedFor = rf.me
	var tm time.Duration = time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
	rf.electTime.Reset(tm)
	rf.currentTerm = rf.currentTerm + 1
	rf.persist()
	rf.voteCounter = 1

	for eachPeer, _ := range rf.peers {
		if rf.me != eachPeer {

			go func(peerInPeers int) {

				args := RequestVoteArgs{}
				reply := RequestVoteReply{}

				rf.mu.Lock()
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				loges := len(rf.log) - 1
				args.LastLogIndex = (loges + rf.snapIndex)
				args.LastLogTerm = rf.log[loges].Term
				rf.mu.Unlock()

				if rf.sendRequestVote(peerInPeers, &args, &reply) {

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.VoteGranted {
						if rf.state == Candidate {
							rf.voteCounter += 1
							if (len(rf.peers) / 2) < rf.voteCounter {
								rf.switchStateTo(Leader)
							}
						}

					} else if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.switchStateTo(Follower)
						rf.persist()
					}

					//rf.mu.Unlock()
				}
			}(eachPeer)

		}

	}
}

func (rf *Raft) heartBeat() {

	for peerInPeers, _ := range rf.peers {
		if rf.me != peerInPeers {
			go rf.recursion(peerInPeers)
		}
	}

}

func (rf *Raft) recursion(peerInPeers int) {
	args := Message{}
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.snapIndex+1 > rf.nextIndex[peerInPeers] {
		rf.mu.Unlock()
		rf.syncSnapshotWith(peerInPeers)
		return
	}

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	entriesInit := make([]LogEntry, len(rf.log[(rf.nextIndex[peerInPeers]-rf.snapIndex):]))
	copy(entriesInit, rf.log[(rf.nextIndex[peerInPeers]-rf.snapIndex):])
	args.Entries = entriesInit
	args.LeaderCommit = rf.commitIndex
	args.PrevLogIndex = rf.nextIndex[peerInPeers] - 1
	args.PrevLogTerm = rf.log[(rf.nextIndex[peerInPeers] - 1 - rf.snapIndex)].Term
	rf.mu.Unlock()

	var reply Message
	if rf.peers[peerInPeers].Call("Raft.AddEntries", &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader {
			//rf.mu.Unlock()
			return
		}
		if reply.Success {
			rf.matchIndex[peerInPeers] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peerInPeers] = rf.matchIndex[peerInPeers] + 1

			for i := (len(rf.log) - 1 + rf.snapIndex); i > rf.commitIndex; i-- {
				var index int = 0
				for _, inter := range rf.matchIndex {
					if inter >= i {
						index = index + 1
					}
				}
				if len(rf.peers) < index*2 {
					rf.updateCommits(i)
					break
				}
			}

		} else {
			if reply.Term <= rf.currentTerm {
				rf.nextIndex[peerInPeers] = reply.IndexForInfoKeeping

				if reply.TermForInfoKeeping == -1 {
					//DPrintf("")
				} else {
					for k := args.PrevLogIndex; k >= rf.snapIndex+1; k-- {
						if rf.log[(k-1-rf.snapIndex)].Term == reply.TermForInfoKeeping {
							rf.nextIndex[peerInPeers] = k
							break
						}
					}
				}
				go rf.recursion(peerInPeers)
			} else {
				rf.currentTerm = reply.Term
				rf.switchStateTo(Follower)
				rf.persist()
			}
		}
		//rf.mu.Unlock()
	}
}

func (rf *Raft) switchStateTo(state int) {

	if state == rf.state {
		return
	}

	rf.state = state

	if state == Candidate {
		rf.electionStarter()
	} else if state == Follower {
		rf.heartTime.Stop()
		var tm time.Duration = time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
		rf.electTime.Reset(tm)
		rf.votedFor = -1
	} else {
		for i := range rf.matchIndex {
			rf.matchIndex[i] = rf.snapIndex
		}
		for i := range rf.nextIndex {
			rf.nextIndex[i] = (len(rf.log) + rf.snapIndex)
		}
		rf.electTime.Stop()
		rf.heartBeat()
		rf.heartTime.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
	}

}

type Message struct {
	Term                int
	Success             bool
	LeaderId            int
	PrevLogIndex        int
	PrevLogTerm         int
	Entries             []LogEntry
	LeaderCommit        int
	TermForInfoKeeping  int
	IndexForInfoKeeping int
}

func (rf *Raft) AddEntries(args *Message, reply *Message) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = true

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	} else if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	var tm time.Duration = time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
	rf.electTime.Reset(tm)

	if rf.snapIndex >= args.PrevLogIndex {
		reply.Success = true
		if args.PrevLogIndex+len(args.Entries) > rf.snapIndex {
			rf.log = rf.log[:1]
			rf.log = append(rf.log, args.Entries[(rf.snapIndex-args.PrevLogIndex):]...)
		}
		return
	}

	reply.Success = false
	if args.PrevLogIndex > (len(rf.log)-1+rf.snapIndex) || rf.log[(args.PrevLogIndex-rf.snapIndex)].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		if args.PrevLogIndex > (len(rf.log) - 1 + rf.snapIndex) {
			reply.IndexForInfoKeeping = len(rf.log)
			reply.TermForInfoKeeping = -1
		} else {
			reply.TermForInfoKeeping = rf.log[(args.PrevLogIndex - rf.snapIndex)].Term
			IndexForInfoKeeping := args.PrevLogIndex
			for rf.log[(IndexForInfoKeeping-1-rf.snapIndex)].Term == reply.TermForInfoKeeping {
				IndexForInfoKeeping--
				if IndexForInfoKeeping-1 == rf.snapIndex {
					break
				}
			}
			reply.IndexForInfoKeeping = IndexForInfoKeeping
		}
		return
	}

	reply.Success = true
	for entr := range args.Entries {
		//fmt.Println("leade11\n")
		if (args.PrevLogIndex + 2 + entr - rf.snapIndex) > len(rf.log) {
			rf.log = append(rf.log[:(args.PrevLogIndex+1+entr-rf.snapIndex)], args.Entries[entr:]...)
			break
		}
		// fmt.Println("leade9\n")
		if args.Entries[entr].Term != rf.log[(args.PrevLogIndex+1+entr-rf.snapIndex)].Term {
			// fmt.Println("leade10\n")
			rf.log = append(rf.log[:(args.PrevLogIndex+1+entr-rf.snapIndex)], args.Entries[entr:]...)
			break
		}

	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < (len(rf.log) - 1 + rf.snapIndex) {
			rf.updateCommits(args.LeaderCommit)
		} else {
			rf.updateCommits((len(rf.log) - 1 + rf.snapIndex))
		}
	}

}

func (rf *Raft) updateCommits(index int) {
	rf.commitIndex = index
	if index > rf.lastApplied {
		go func(index int, entriesMass []LogEntry) {
			for entrInd, entr := range entriesMass {

				rf.mu.Lock()
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entr.Command,
					CommandIndex: index + entrInd,
				}

				if msg.CommandIndex >= rf.lastApplied {
					rf.lastApplied = msg.CommandIndex
				}
				rf.applyCh <- msg
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, append([]LogEntry{}, rf.log[(rf.lastApplied+1-rf.snapIndex):(rf.commitIndex+1-rf.snapIndex)]...))
	}
}

func (rf *Raft) ChangeSnap(index int, byteMas []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapIndex {
		return
	}
	rf.log = rf.log[(index - rf.snapIndex):]
	rf.snapIndex = index
	rf.persister.SaveStateAndSnapshot(rf.codeRaft(), byteMas)

	for k := range rf.peers {
		if rf.me == k {
			continue
		}
		go rf.syncSnapshotWith(k)
	}
}

func (rf *Raft) syncSnapshotWith(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	snap := Snap{}
	snap.Term = rf.currentTerm
	snap.LeaderId = rf.me
	snap.LastIndex = rf.snapIndex
	snap.LastTerm = rf.log[0].Term
	snap.DataBase = rf.persister.ReadSnapshot()
	rf.mu.Unlock()

	var response Snap

	if rf.peers[server].Call("Raft.AddSnap", &snap, &response) {
		rf.mu.Lock()
		if response.Term <= rf.currentTerm {
			if rf.matchIndex[server] < snap.LastIndex {
				rf.matchIndex[server] = snap.LastIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1

		} else {
			rf.currentTerm = response.Term
			rf.switchStateTo(Follower)
			rf.persist()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) AddSnap(args *Snap, reply *Snap) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIndex < rf.snapIndex {
		return
	}

	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}

	if len(rf.log) > (args.LastIndex-rf.snapIndex) && rf.log[(args.LastIndex-rf.snapIndex)].Term == args.LastTerm {
		rf.log = rf.log[(args.LastIndex - rf.snapIndex):]
	} else {
		rf.log = []LogEntry{{Term: args.LastTerm, Command: nil}}
	}

	if rf.commitIndex < args.LastIndex {
		rf.snapIndex = args.LastIndex
		rf.commitIndex = args.LastIndex
	}
	if rf.lastApplied < args.LastIndex {
		rf.snapIndex = args.LastIndex
		rf.lastApplied = args.LastIndex
	}
	rf.snapIndex = args.LastIndex

	rf.persister.SaveStateAndSnapshot(rf.codeRaft(), args.DataBase)

	if rf.lastApplied > rf.snapIndex {
		return
	}

	addSnap := ApplyMsg{}
	addSnap.CommandIndex = rf.snapIndex
	addSnap.Command = "AddSnap"
	addSnap.CommandValid = false
	addSnap.CommandBytes = rf.persister.ReadSnapshot()

	go func(apMs ApplyMsg) {
		rf.applyCh <- apMs
	}(addSnap)
}
