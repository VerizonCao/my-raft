package kvraft

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//具体的的操作指令
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Kind  string //"Put" or "Append" "Get"
	Key   string
	Value string
	Id    int64 //clerk的id
	ReqId int   // 这个clerk的顺序
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft         //自己绑定的raft
	applyCh chan raft.ApplyMsg //一个 Msg的 chan
	dead    int32              // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	db     map[string]string //database  key-value pair
	ack    map[int64]int     //这个记录了每个clerk的最后一次的申请reqId  需要递增
	result map[int]chan Op   //对应于每个log的回应
}

func (kv *KVServer) AppendEntryToLog(entry Op) bool {
	//开始发送  得到index
	index, _, isLeader := kv.rf.Start(entry)

	//如果不是leader 直接返回false
	if !isLeader {
		return false
	}

	kv.mu.Lock()

	//看看有没有这个index 这里也即是说 raft的log顺序和这个resutl的顺序一样
	//加锁避免和下方的类似操作重复
	ch, ok := kv.result[index]

	//确保初始化
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <-ch:
		return op == entry //保持一致
	case <-time.After(1000 * time.Millisecond): //等待 raft来回复。最多等待 1000ms
		return false
	}
}
func (kv *KVServer) CheckDup(id int64, reqid int) bool {
	v, ok := kv.ack[id] //得到该client的最后一个 reqId
	if ok {
		return v >= reqid //他比我的新 那么就是重复了
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//新的op  除了value 全部继承 args
	entry := Op{Kind: "Get", Key: args.Key, Id: args.Id, ReqId: args.ReqID}

	ok := kv.AppendEntryToLog(entry) //同步  让所有的server 来同步

	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.WrongLeader = false
		reply.Err = OK

		//操作database 需要加锁 避免误操作
		kv.mu.Lock()
		reply.Value = kv.db[args.Key] //value从这里拿
		if !kv.CheckDup(args.Id, args.ReqID) {
			kv.ack[args.Id] = args.ReqID //记录reqId 表示成功处理
		}
		kv.mu.Unlock()
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{Kind: args.Op, Key: args.Key, Value: args.Value, Id: args.Id, ReqId: args.ReqID}
	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
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

func (kv *KVServer) Apply(args Op) {

	//就是操作 ack 写入 和 对于数据库写入的操作

	switch args.Kind {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	}
	kv.ack[args.Id] = args.ReqId

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

	kv.db = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.result = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) //这里很关键 得到 所有的server 自己的序号 persistent 保存 参数 和 applyChannel

	go func() { //需要server端始终运行。来接受命令，处理msg
		for {
			//首先 拿取 msg
			msg := <-kv.applyCh
			if msg.UseSnapshot { //当收到snap的msg时候，解压得到index和term，制作db 和 ack。这个是上线后初始化一个server的时候用的
				var LastIncludedIndex int
				var LastIncludedTerm int

				r := bytes.NewBuffer(msg.Snapshot)
				d := gob.NewDecoder(r)

				kv.mu.Lock()
				d.Decode(&LastIncludedIndex)
				d.Decode(&LastIncludedTerm)
				kv.db = make(map[string]string)
				kv.ack = make(map[int64]int)
				d.Decode(&kv.db)
				d.Decode(&kv.ack)
				kv.mu.Unlock()
			} else {
				//分离op
				op := msg.Command.(Op)
				kv.mu.Lock()

				//看看这个 是否已经被加入ack
				if !kv.CheckDup(op.Id, op.ReqId) {
					kv.Apply(op)
				}

				//得到那个op的chan
				ch, ok := kv.result[msg.CommandIndex]
				if ok {
					//反正确保是空的
					select {
					case <-kv.result[msg.CommandIndex]:
					default:
					}
					ch <- op
				} else {
					kv.result[msg.CommandIndex] = make(chan Op, 1)
				}

				//当log存储的大小超过限制   we need snapshot   记录关键信息
				if maxraftstate != -1 && kv.rf.GetPerisistSize() > maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.db)
					e.Encode(kv.ack)
					data := w.Bytes()
					//开启一个snapshotting  消除自己的log
					go kv.rf.StartSnapshot(data, msg.CommandIndex)
				}
				kv.mu.Unlock()
			}
		}

	}()

	return kv
}
