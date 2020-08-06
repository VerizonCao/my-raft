package shardkv

import "my-raft/shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me    int64
	ReqId int64
	Shard int
}

type PutAppendReply struct {
	Err         Err
	WrongLeader bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Shard int
}

type GetReply struct {
	Err         Err
	Value       string
	WrongLeader bool
}


//add
type ReqShared struct {
	Shards []int
	ConfigNum int
}

type RespShared struct {
	Successed bool
	ConfigNum  int
	Group      int
	Data    map[int]map[string]string
	MsgIDs  map[int64] int64
}

type RespShareds struct {
	ConfigNum  int
}

type ReqDeleteShared struct {
	Shards []int
	ConfigNum int
}

type RespDeleteShared struct {
	Shard int
	Config shardmaster.Config
}



//func
func (kv *ShardKV) isLeader() bool  {
	_,rst := kv.rf.GetState()
	return rst
}
