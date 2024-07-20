package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string

	Uuid     int64
	ClientId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	Uuid     int64
	ClientId int
}

type GetReply struct {
	Err   Err
	Value string
}
