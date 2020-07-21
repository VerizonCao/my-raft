package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"../labrpc"
)

//是发起请求的结构体
type Clerk struct {
	servers []*labrpc.ClientEnd //所有可以连接的server
	// You will have to modify this struct.
	id    int64
	reqid int //  请求 id  递增
	mu    sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// create a clerk
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand() //id 随机选取
	ck.reqid = 0    //reqid  开始设为 0
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
	//试图 去  得到一个value

	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	args.Id = ck.id //代表
	ck.mu.Lock()
	args.ReqID = ck.reqid
	ck.reqid++ //让这个clerk的 req id 递增
	ck.mu.Unlock()

	for { //没有返回的话  始终无限次发送消息
		for i, _ := range ck.servers { //对于全体server
			var reply GetReply
			//  那个server 来调用 get操作   这是同步的  如果一下子没得到回复 会卡在这里 知道 超时
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply) //传送到 RaftKV的Get函数
			if ok && reply.WrongLeader == false {
				return reply.Value 
			}
		}
	}

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

//clerk本质是一定要得到结果 否则一直询问
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.Id = ck.id
	ck.mu.Lock()
	args.ReqID = ck.reqid
	ck.reqid++
	ck.mu.Unlock()
	for {
		for _,v := range ck.servers {
			var reply PutAppendReply
			ok := v.Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
