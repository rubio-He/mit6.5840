package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	clientId int
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
	ck.leaderId = int(nrand()) % len(ck.servers)
	ck.clientId = int(nrand())
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	reply := GetReply{}
	i := ck.leaderId
	uuid := nrand()
	DPrintf("Start Get to Leader")
	for {
		ok := ck.servers[i].Call("KVServer.Get", &GetArgs{key, uuid, ck.clientId}, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			continue
		}
		break
	}
	ck.leaderId = i
	if reply.Err == ErrNoKey {
		return ""
	}
	DPrintf("Receive Get from Leader %d", ck.leaderId)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	reply := PutAppendReply{}
	i := ck.leaderId
	uuid := nrand()
	DPrintf("%s, %s put/Append to leader", key, value)
	for {
		ok := ck.servers[i].Call("KVServer."+op, &PutAppendArgs{Key: key, Value: value, Uuid: uuid, ClientId: ck.clientId}, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			continue
		}
		break
	}
	ck.leaderId = i
	DPrintf("Receive Put/Append from Leader %d", ck.leaderId)
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Start Put")
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("Start Append")
	ck.PutAppend(key, value, "Append")
}
