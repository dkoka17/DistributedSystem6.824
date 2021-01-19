package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
    "time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Key   string
    Value string
    Name  string
    ClientId  int64
    RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    data map[string]string
    requestLogs map[int64]int
    controller map[int]chan LogMsg              
}

type LogMsg struct {
    ClientId  int64
    RequestId int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    opObject := Op{}
    opObject.Key = args.Key
    opObject.Name = "Get"
    opObject.ClientId = args.ClientId
    opObject.RequestId = args.RequestId

    if !kv.waitForServerResponse(opObject, 500*time.Millisecond) {
        reply.LeaderChecker=false
        kv.mu.Lock()
        resp, status := kv.data[args.Key]
        kv.mu.Unlock()
        if status {
            reply.Value = resp
            return
        }
        reply.Err = ErrNoKey
    }else{
        reply.LeaderChecker=true
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    opObject := Op{}
    opObject.Key = args.Key
    opObject.Name = args.Op
    opObject.Value = args.Value
    opObject.ClientId = args.ClientId
    opObject.RequestId = args.RequestId
    reply.LeaderChecker = kv.waitForServerResponse(opObject, 500*time.Millisecond)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
    kv.data = make(map[string]string)
    kv.controller = make(map[int]chan LogMsg)
    kv.requestLogs = make(map[int64]int)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
    go func() {
        for chane := range kv.applyCh {
            if !chane.CommandValid {
                continue
            }
            op := chane.Command.(Op)
            kv.mu.Lock()
            lastLog, status := kv.requestLogs[op.ClientId]
            if status && op.RequestId<=lastLog {
                kv.mu.Unlock()
                continue
            }

            if op.Name == "Put" {
                kv.data[op.Key]=op.Value
            }else if op.Name == "Append"{
                kv.data[op.Key]=kv.data[op.Key]+op.Value    
            }          
            kv.requestLogs[op.ClientId] = op.RequestId

            if cha, status := kv.controller[chane.CommandIndex]; status {
                msg := LogMsg{}
                msg.ClientId=op.ClientId
                msg.RequestId=op.RequestId
                cha<-msg
            }
            kv.mu.Unlock()
        }
    }()

	return kv
}

func (kv *KVServer) waitForServerResponse(op Op, timeout time.Duration) bool {
    var response bool
    index, _, isLeader := kv.rf.Start(op)
    if isLeader == false {
        return true
    }
    kv.mu.Lock()
    if _, status := kv.controller[index]; !status {
        kv.controller[index] = make(chan LogMsg, 1)
    }
    ch := kv.controller[index]
    kv.mu.Unlock()
    select {
        case <-time.After(timeout):
            kv.mu.Lock()
            lastLog, status := kv.requestLogs[op.ClientId]
            if !status || op.RequestId>lastLog {
                response = true
            }else{
                response = false
            } 
            kv.mu.Unlock()
        case notify := <-ch:
            if notify.ClientId==op.ClientId && notify.RequestId==op.RequestId {
                response = false
            } else {
                response = true
            }
    }  
    kv.mu.Lock()
    delete(kv.controller, index)
    kv.mu.Unlock()
    return response
}