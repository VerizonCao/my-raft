package shardkv

// import "../shardmaster"
import (
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Command interface{}
	Ch      chan (interface{})
}


func init(){
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(ReqShared{})
	labgob.Register(RespShared{})
	labgob.Register(RespShareds{})
	labgob.Register(ReqDeleteShared{})
	labgob.Register(RespDeleteShared{})
	labgob.Register(shardmaster.Config{})
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd //方法 根据server id 来制作end
	gid          int                            //从属的group
	masters      []*labrpc.ClientEnd            //servers  group内的
	maxraftstate int                            // snapshot if log grows this big

	// Your definitions here.
	kvs           [shardmaster.NShards]map[string]string    //每个shard的database
	reqIDs        map[int64]int64    //每个client的reqId
	killChan      chan (bool)
	killed        bool
	persister     *raft.Persister
	logApplyIndex int

	config         shardmaster.Config     //一个config  猜想和client一样是最新的
	nextConfig     shardmaster.Config     //下一个config  需要update
	notReadyShards map[int][]int          //出现问题的shards 比如超过一半的raft down 或者 处于迁徙过程中 gid -> shard ids

	deleteShardsNum int
	mck             *shardmaster.Clerk //连接master的client
	timer           *time.Timer    //定期来check config 变动  修改内容
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	for i:=0;i<shardmaster.NShards;i++ {   //初始化全体shards的数据库
		kv.kvs[i] = make(map[string]string)
	}
	kv.reqIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool),1)
	kv.persister = persister
	kv.logApplyIndex = 0


	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//设置server的添加参数
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config.Num = 0
	kv.nextConfig.Num = 0
	kv.notReadyShards = make(map[int] []int)
	kv.deleteShardsNum = 0
	kv.killed = false
	kv.timer = time.NewTimer(time.Duration(time.Millisecond * 100))
	go kv.mainLoop()
	go kv.shardLoop()

	return kv
}


func (kv *ShardKV) shardLoop() {
	for !kv.killed {
		if !kv.isLeader() {
			time.Sleep(time.Millisecond*50)
			continue
		}
		isUpdated,shards := kv.isUpdateConfig()
		if !isUpdated {
			time.Sleep(time.Millisecond*50)
			continue
		}
		config := kv.nextConfig
		Num := config.Num
		waitCh := make(chan bool,len(shards))
		for key,value := range shards {  //遍历所有分片，请求数据
			go kv.getShardFromOther(key,value,waitCh,Num)
		}
		for !kv.killed && kv.isLeader()  {
			if kv.cofigCompleted(Num) {
				break
			}
			if kv.isReadyShards(-1) {
				break
			}
			select  {
			case <- waitCh :
			case <-time.After(time.Millisecond * 500): //超时
			}
		}
		if !kv.isReadyShards(-1) {
			time.Sleep(time.Millisecond*50)
			continue
		}
		//获取的状态写入RAFT，直到成功
		for !(kv.cofigCompleted(Num)) && !kv.killed && kv.isLeader() {
			respShards := RespShareds{
				ConfigNum : Num ,
			}
			kv.startShard(&respShards)
		}
		time.Sleep(time.Millisecond*100)
	}
}

//主逻辑  apply  监测 config 变动 来调整内容
func (kv *ShardKV) mainLoop() {
	duration := time.Duration(time.Millisecond * 40)
	for !kv.killed {
		select {
		case <-kv.killChan:
			return
		case msg := <-kv.applyCh:   //raft同步完 server来操作
			kv.onApply(msg)
		case <-kv.timer.C :   //让raft来同步config
			kv.updateConfig()
			kv.timer.Reset(duration)
		}
	}
}


func (kv *ShardKV) onApply(applyMsg raft.ApplyMsg) {

}

//跟新config  如果是leader 如果下一个config是config的num
func (kv *ShardKV) updateConfig()  {
	if _, isLeader := kv.rf.GetState(); isLeader {
		if kv.nextConfig.Num == kv.config.Num {
			//请求master的client 来得到config
			config := kv.mck.Query(kv.nextConfig.Num+1)
			kv.startConfig(&config)  //然后开始处理config
		}
	}
}


func (kv *ShardKV) startConfig(config *shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//当得到的config num 大于 next  并且  下一个的等于现在的这个
	if config.Num > kv.nextConfig.Num && kv.nextConfig.Num == kv.config.Num {
		op := Op {
			Command : *config, //请求数据
			Ch : make(chan(interface{})), //日志提交chan
		}
		kv.rf.Start(op) //写入Raft  开始同步
	}
}


func (kv *ShardKV) isUpdateConfig()  (bool,map[int][]int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rst := make(map[int][]int)
	for key,value := range kv.notReadyShards {   //返回没有准备好的shard的copy
		rst[key] = value
	}
	return kv.nextConfig.Num > kv.config.Num,rst   //判断下一个config的num是否大于 目前的
}


//得到shard 从别的 group？
func (kv *ShardKV) getShardFromOther(group int,shards []int,ch chan bool,num int)   {
	defer func() {ch<-true}()
	complet := false
	var resp RespShared
	for !complet && !(kv.cofigCompleted(num)) && !kv.killed {
		servers, ok := kv.config.Groups[group]  //获取目标组服务
		if !ok  {
			time.Sleep(time.Millisecond * 500)
			continue
		}
		req := ReqShared  {
			ConfigNum : kv.nextConfig.Num,
			Shards : shards,
		}
		for i := 0; i < len(servers); i++ {
			server := kv.make_end(servers[i])
			ok := server.Call("ShardKV.GetShard", &req, &resp)
			if ok && resp.Successed {
				complet = true
				break
			}
			if kv.cofigCompleted(num) || kv.killed{
				break
			}
			time.Sleep(time.Millisecond*10)
		}
	}
	//存储该分片数据
	for !kv.killed && !kv.cofigCompleted(num) && !kv.isReadyShards(group) && kv.isLeader() {
		successed,_ := kv.opt(resp)
		if   successed {
			break
		}
	}
}




