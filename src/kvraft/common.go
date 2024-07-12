package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrKilled      = "ErrKilled"
	ErrPartioned   = "ErrPartioned"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Uuid  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	Uuid int64
}

type GetReply struct {
	Err      Err
	LeaderId int
	Value    string
}
