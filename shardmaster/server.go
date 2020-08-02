package shardmaster

import (
	"log"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

func init() {
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

//shard管理器  带有一个raft ？？
type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	ReqIDs   map[int64]int64 //client -> reqId
	shutdown chan struct{}

	configs []Config // indexed by config num
}

type Op struct {
	Command interface{}
	Ch      chan (interface{})
}

//check repeated RPC 包含写入功能
func (sm *ShardMaster) isRepeated(client int64, msgId int64, update bool) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	rst := false
	//看看有没有这个client   如果没有 表示还没有这个client
	index, ok := sm.ReqIDs[client]
	if ok {
		//判断是否重复
		rst = index >= msgId
	}
	//需要update 且不重复  写入
	if update && !rst {
		sm.ReqIDs[client] = msgId
	}
	return rst
}

//raft同步操作  封装args为op   检测op的chan
func (sm *ShardMaster) opt(clientId int64, reqId int64, req interface{}) (bool, interface{}) {
	//之前有的RPC请求，并且msg处于正常的位置，返回nil
	if reqId > 0 && sm.isRepeated(clientId, reqId, false) {
		return true, nil
	}

	op := Op{
		Command: req,                      //请求数据
		Ch:      make(chan (interface{})), //日志提交chan  监测 结果
	}
	_, _, isLeader := sm.rf.Start(op) // server会加入一谢信息
	if !isLeader {
		return false, nil //判定是否是master的leader
	}
	select {
	case resp := <-op.Ch:
		return true, resp
	case <-time.After(time.Millisecond * 800): //超时
	}
	return false, nil
}

//写入config信息  越界则返回最新的
func (sm *ShardMaster) getConfig(index int, config *Config) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if index >= 0 && index < len(sm.configs) {
		*config = sm.configs[index]
		return true
	}
	*config = sm.configs[len(sm.configs)-1]
	return false
}

//得到最新的config
func (sm *ShardMaster) getCurrentConfig() Config {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	config := sm.configs[len(sm.configs)-1]
	CopyGroups(&config, sm.configs[len(sm.configs)-1].Groups) //貌似是group不深度拷贝。
	return config
}

//加入一个包含这些server的group
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	ok, _ := sm.opt(args.Me, args.ReqId, *args)
	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	ok, _ := sm.opt(args.Me, args.ReqId, *args)
	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	ok, _ := sm.opt(args.Me, args.ReqId, *args)
	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	//得到了config
	if sm.getConfig(args.Num, &reply.Config) {
		reply.WrongLeader = false
		return
	}
	//没得到 先解决同步性问题，然后返回
	ok, resp := sm.opt(-1, -1, *args)
	if ok {
		reply.Config = resp.(Config)
	}
	reply.WrongLeader = !ok
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.shutdown)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos ? Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {

	sm := new(ShardMaster)
	sm.me = me

	//初始config和group
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})

	sm.applyCh = make(chan raft.ApplyMsg) //raft回复chan
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.ReqIDs = make(map[int64]int64)
	sm.shutdown = make(chan (struct{}))

	// Your code here.

	//main loop
	go func() {
		for {
			select {
			case <-sm.shutdown: //用来关机下线  之前的server都是test操作下线的
				return
			case msg := <-sm.applyCh: //raft传递信息了
				sm.onApply(msg)
			}
		}
	}()

	return sm
}

//server得到raft消息了 然后开始处理  然后加入op.chan
func (sm *ShardMaster) onApply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid { //非状态机apply消息
		return
	}
	op := applyMsg.Command.(Op)
	var resp interface{}
	if command, ok := op.Command.(JoinArgs); ok {
		resp = sm.join(&command)
	} else if command, ok := op.Command.(LeaveArgs); ok {
		resp = sm.leave(&command)
	} else if command, ok := op.Command.(MoveArgs); ok {
		resp = sm.move(&command)
	} else {
		command := op.Command.(QueryArgs) //断言 是queryArgs
		resp = sm.query(&command)
	}
	select {
	case op.Ch <- resp:
	default:
	}
}

func (sm *ShardMaster) join(args *JoinArgs) bool {

	if sm.isRepeated(args.Me, args.ReqId, true) {
		return true
	}
	config := sm.getCurrentConfig()
	if config.Num == 0 { //如果第一次配置，则重分配
		config.Groups = args.Servers
		DistributionGroups(&config) //重分配分片与组
	} else {
		MergeGroups(&config, args.Servers) //将新的组加入，调整分片
	}
	sm.appendConfig(&config) //加入config
	return true
}

func (sm *ShardMaster) leave(args *LeaveArgs) bool {
	if sm.isRepeated(args.Me, args.ReqId, true) {
		return true
	}
	config := sm.getCurrentConfig()
	DeleteGroups(&config, args.GIDs) //删除组
	sm.appendConfig(&config)
	return true
}

func (sm *ShardMaster) move(args *MoveArgs) bool {
	if sm.isRepeated(args.Me, args.ReqId, true) {
		return true
	}
	config := sm.getCurrentConfig()      //得到最新的复制品
	config.Shards[args.Shard] = args.GID //修改 shard的归属为这个gid
	sm.appendConfig(&config)             //然后加入config
	return true
}

func (sm *ShardMaster) query(args *QueryArgs) Config {
	reply := Config{}
	sm.getConfig(args.Num, &reply)
	return reply
}

func (sm *ShardMaster) appendConfig(config *Config) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	config.Num = len(sm.configs)
	sm.configs = append(sm.configs, *config)

}
