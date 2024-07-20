package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	PutOp    OpType = "Put"
	AppendOp OpType = "Append"
	GetOp    OpType = "Get"
)

type Op struct {
	Idx   int
	Type  OpType
	Key   string
	Value string

	Uuid     int64
	ClientId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	newestOpIdx int
	kvmap       map[string]string
	opmap       map[int64]Op
	opChan      map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	index := kv.newestOpIdx + 1
	op := Op{Idx: index, Type: GetOp, Key: args.Key, Value: "", Uuid: args.Uuid, ClientId: args.ClientId}
	if _, ok := kv.opChan[args.ClientId]; !ok {
		kv.opChan[args.ClientId] = make(chan Op)
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("kv %d receive Get %+v", kv.me, args)
	opChan := kv.opChan[args.ClientId]
	kv.mu.Unlock()
	for {
		select {
		case appliedOp := <-opChan:
			if appliedOp.Uuid != args.Uuid {
				DPrintf("skip applied Op %+v", appliedOp)
				continue
			}
			reply.Err = OK
			reply.Value = appliedOp.Value
			return
		default:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				DPrintf("kv %d not a leader anymore, return", kv.me)
				reply.Err = ErrWrongLeader
				return
			}
		}
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	index := kv.newestOpIdx + 1
	op := Op{Idx: index, Type: PutOp, Key: args.Key, Value: args.Value, Uuid: args.Uuid, ClientId: args.ClientId}
	if _, ok := kv.opChan[args.ClientId]; !ok {
		kv.opChan[args.ClientId] = make(chan Op)
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("kv %d receive Put %+v", kv.me, args)
	opChan := kv.opChan[args.ClientId]
	kv.mu.Unlock()

	for {
		select {
		case appliedOp := <-opChan:
			DPrintf("put applied Op %+v", appliedOp)
			if appliedOp.Uuid != args.Uuid {
				DPrintf("skip applied Op %+v", appliedOp)
				continue
			}
			reply.Err = OK
			return
		default:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				DPrintf("kv %d not a leader anymore, return", kv.me)
				reply.Err = ErrWrongLeader
				return
			}
		}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	index := kv.newestOpIdx + 1
	op := Op{Idx: index, Type: AppendOp, Key: args.Key, Value: args.Value, Uuid: args.Uuid, ClientId: args.ClientId}
	if _, ok := kv.opChan[args.ClientId]; !ok {
		kv.opChan[args.ClientId] = make(chan Op)
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("kv %d receive Append %+v", kv.me, args)
	opChan := kv.opChan[args.ClientId]
	kv.mu.Unlock()

	for {
		select {
		case appliedOp := <-opChan:
			if appliedOp.Uuid != args.Uuid {
				DPrintf("skip applied Op %+v", appliedOp)
				continue
			}
			reply.Err = OK
			return
		default:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				DPrintf("kv %d not a leader anymore, return", kv.me)
				reply.Err = ErrWrongLeader
				return
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.newestOpIdx = 0
	kv.opmap = make(map[int64]Op)
	kv.kvmap = make(map[string]string)
	kv.opChan = make(map[int]chan Op)
	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			DPrintf("kv %d Receive Applied Op %+v", kv.me, op)
			kv.mu.Lock()
			switch op.Type {
			case GetOp:
				if _, ok := kv.opmap[op.Uuid]; !ok {
					kv.opmap[op.Uuid] = op
				}
				op.Value = kv.kvmap[op.Key]
				if _, isLeader := kv.rf.GetState(); isLeader {
					DPrintf("kv %d Sent", kv.me)
					kv.opChan[op.ClientId] <- op
				}
			case PutOp:
				if _, ok := kv.opmap[op.Uuid]; !ok {
					kv.opmap[op.Uuid] = op
					kv.kvmap[op.Key] = op.Value
				}
				if _, isLeader := kv.rf.GetState(); isLeader {
					DPrintf("kv %d Sent", kv.me)
					kv.opChan[op.ClientId] <- op
				}
			case AppendOp:
				if _, ok := kv.opmap[op.Uuid]; !ok {
					kv.opmap[op.Uuid] = op
					kv.kvmap[op.Key] += op.Value
				}
				if _, isLeader := kv.rf.GetState(); isLeader {
					DPrintf("kv %d Sent", kv.me)
					kv.opChan[op.ClientId] <- op
				}
			}
			kv.mu.Unlock()
		}
	}
}
