package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
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
	for {
		DPrintf("Start Get to Leader %d", ck.leaderId)
		ok := ck.servers[i].Call("KVServer.Get", &GetArgs{key, uuid}, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrPartioned {
			continue
		}
		break
	}
	ck.leaderId = i
	if reply.Err == ErrNoKey {
		return ""
	}
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
	for {
		DPrintf("%s, %s put to leader %d", key, value, i)
		ok := ck.servers[i].Call("KVServer."+op, &PutAppendArgs{Key: key, Value: value, Uuid: uuid}, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrPartioned {
			continue
		}
		break
	}
	ck.leaderId = i
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Start Put")
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("Start Append")
	ck.PutAppend(key, value, "Append")
}
