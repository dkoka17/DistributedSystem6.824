package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    lastId int
    leaderId int
    clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
    ck.clientId = nrand() 
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
    //requestId := ck.lastId + 1

    for {
        var response GetReply
        getArg := GetArgs{}
        getArg.Key = key
        getArg.ClientId = ck.clientId
        getArg.RequestId = ck.lastId + 1

        status := ck.servers[ck.leaderId].Call("KVServer.Get", &getArg, &response)
        if (!status||response.LeaderChecker) {
            ck.leaderId=(ck.leaderId+1)%len(ck.servers)
            continue
        } 
        ck.lastId=ck.lastId+1
        return response.Value
    }
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    for {
        var response PutAppendReply

        appendObj:=PutAppendArgs{}
        appendObj.Key=key
        appendObj.ClientId=ck.clientId
        appendObj.RequestId=ck.lastId+1
        appendObj.Value=value
        appendObj.Op=op

        ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &appendObj, &response)
        if (!ok||response.LeaderChecker) {
            ck.leaderId=(ck.leaderId+1)%len(ck.servers)
            continue
        }
        ck.lastId=ck.lastId+1
        return
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}