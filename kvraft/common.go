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
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
	ReqID int
}

type PutAppendReply struct {
	Err Err
	WrongLeader bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id    int64 //clerk 的 id
	ReqID int
}

type GetReply struct {
	WrongLeader bool //if this is a wrong leader
	Err         Err
	Value       string
}
