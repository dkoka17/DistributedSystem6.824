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

import "sync"
import "sync/atomic"
import "../labrpc"
import "math/rand"
import "time"

// import "bytes"
// import "../labgob"



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
}

type LogEntry struct {
    Command interface{}
    Term    int
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

	electTime *time.Timer 
    heartTime *time.Timer 
    state int         
    voteCounter int   
     
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var term int
    var isleader bool
    // Your code here (2A).
    rf.mu.Lock()
    isleader = false
    if(rf.state == Leader){
        isleader = true
    }
    term = rf.currentTerm
    rf.mu.Unlock()
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
    reply.VoteGranted = false
    reply.Term = rf.currentTerm
    
    if args.Term < rf.currentTerm {
        return
    }else  if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.switchStateTo(Follower)
    }


    if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
        reply.VoteGranted = true
        rf.votedFor = args.CandidateId
    }
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
                    } else if rf.state == Candidate{
                        rf.mu.Lock()
                        rf.electionStarter()
                        rf.mu.Unlock()
                    }
                
            }
        }
    }()

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())


    return rf
}

func (rf *Raft) electionStarter() {

    rf.votedFor = rf.me 
    var tm time.Duration = time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond          
    rf.electTime.Reset(tm)
    rf.currentTerm = rf.currentTerm + 1
    rf.voteCounter = 1

    for peerInPeers, _ := range rf.peers {
        if rf.me != peerInPeers  {

            go func(peerInPeers int) {
                
                args := RequestVoteArgs{}
                reply := RequestVoteReply{}

                rf.mu.Lock()
                args.Term = rf.currentTerm
                args.CandidateId = rf.me
                rf.mu.Unlock()
    
                if rf.sendRequestVote(peerInPeers, &args, &reply) {
                   
                    rf.mu.Lock()
                    if reply.VoteGranted {
                        if rf.state == Candidate {
                            rf.voteCounter += 1
                            if (len(rf.peers)/2) < rf.voteCounter  {
                                rf.switchStateTo(Leader)
                            }
                        }
                        
                    } else if rf.currentTerm < reply.Term {
                        rf.currentTerm = reply.Term
                        rf.switchStateTo(Follower)
                    }

                    rf.mu.Unlock()
                }
            }(peerInPeers)
            
        }

    }
}

func (rf *Raft) heartBeat() {

    for peerInPeers, _ := range rf.peers {
        if rf.me != peerInPeers {
            
            go func(peerInPeers int) {

                args := Message{}
                rf.mu.Lock()
                args.Term = rf.currentTerm
                args.LeaderId = rf.me
                rf.mu.Unlock()

                
                if rf.peers[peerInPeers].Call("Raft.AddEntries", &args, &args){
                    
                    rf.mu.Lock()
                    if args.Term > rf.currentTerm {
                        rf.currentTerm = args.Term
                        rf.switchStateTo(Follower)
                    }
                    rf.mu.Unlock()

                }
            }(peerInPeers)
        }
    }

    time.Sleep(HEART_BEAT_TIMEOUT * time.Millisecond)

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
	}else {
		rf.electTime.Stop()
            rf.heartBeat()
            rf.heartTime.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
    }	
	
}

type Message struct {
    Term    int  
    Success bool 
    LeaderId    int
}

func (rf *Raft) AddEntries(args *Message, reply *Message) {
   
    rf.mu.Lock()
    defer rf.mu.Unlock()
    args.Success = true

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.switchStateTo(Follower)
        var tm time.Duration = time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
        rf.electTime.Reset(tm)
    } else if args.Term < rf.currentTerm {
        args.Success = false
        args.Term = rf.currentTerm        
    
    }
    return
    
}
