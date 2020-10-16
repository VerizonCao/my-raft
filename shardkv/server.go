package shardkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

func init() {
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(ReqShards{})
	labgob.Register(RespShards{})
	labgob.Register(RespNextConfig{})
	labgob.Register(ReqDeleteShared{})
	labgob.Register(RespDeleteShared{})
	labgob.Register(shardmaster.Config{})
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command interface{}
	Ch      chan (interface{})
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd //其他组的server
	gid          int                            //从属的group
	masters      []*labrpc.ClientEnd            //master 的 clerk
	maxraftstate int                            // snapshot if log grows this big

	// Your definitions here.
	kvs       [shardmaster.NShards]map[string]string //每个shard的database
	reqIDs    map[int64]int64                        //每个client的reqId
	killChan  chan (bool)
	killed    bool
	persister *raft.Persister
	// logApplyIndex int

	config         shardmaster.Config //config
	nextConfig     shardmaster.Config //下一个config  需要update
	notReadyShards map[int][]int      //每个group内我没有的但是需要的shards

	deleteShardsNum int
	mck             *shardmaster.Clerk //连接master的client
	timer           *time.Timer        //定期来check config 变动  修改内容
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ok, value := kv.opt(*args)
	reply.WrongLeader = !ok
	if ok {
		*reply = value.(GetReply)
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ok, value := kv.opt(*args)
	reply.WrongLeader = !ok
	if ok {
		reply.Err = value.(Err)
	}
}

func (kv *ShardKV) GetShard(req *ReqShards, reply *RespShards) { //得到shards
	ok, value := kv.opt(*req)
	reply.Successed = false
	if ok {
		*reply = value.(RespShards)
	}
}

func (kv *ShardKV) DeleteShards(req *ReqDeleteShared, resp *RespDeleteShared) {
	if req.ConfigNum > kv.deleteShardsNum {
		if kv.config.Num == req.ConfigNum || kv.config.Num == req.ConfigNum+1 {
			kv.deleteShardsNum = req.ConfigNum
			kv.opt(*req)
		}
	}
}

func (kv *ShardKV) opt(req interface{}) (bool, interface{}) { //同步raft
	op := Op{
		Command: req,                         //请求数据
		Ch:      make(chan (interface{}), 1), //日志提交chan
	}
	_, _, isLeader := kv.rf.Start(op) //写入Raft
	if !isLeader {                    //判定是否是leader
		return false, nil
	}
	select {
	case resp := <-op.Ch:
		return true, resp
	case <-time.After(time.Millisecond * 1000): //超时
	}
	return false, nil
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.killed = true
	kv.rf.Kill() //关闭raft
	kv.killChan <- true
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

	for i := 0; i < shardmaster.NShards; i++ { //初始化全体shards的数据库
		kv.kvs[i] = make(map[string]string)
	}
	kv.reqIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool), 1)
	kv.persister = persister
	// kv.logApplyIndex = 0

	// Use something like this to talk to the shardmaster:

	//设置server的添加参数
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config.Num = 0
	kv.nextConfig.Num = 0
	kv.notReadyShards = make(map[int][]int)
	kv.deleteShardsNum = 0
	kv.killed = false
	kv.timer = time.NewTimer(time.Duration(time.Millisecond * 100))
	go kv.mainLoop()
	go kv.shardLoop()
	return kv
}

//主逻辑  apply  1 基本的业务检测    2 按时检测config的变动，开始同步集群内的config。    //////////////////////
func (kv *ShardKV) mainLoop() {
	duration := time.Duration(time.Millisecond * 40)
	for !kv.killed {
		select {
		case <-kv.killChan:
			return
		case msg := <-kv.applyCh: //raft同步完 server来操作
			kv.onApply(msg)

		case <-kv.timer.C: //让raft来同步config
			kv.updateConfig()
			kv.timer.Reset(duration)
		}

	}

}

// 主函数判断是否需要跟新config
func (kv *ShardKV) updateConfig() {
	if _, isLeader := kv.rf.GetState(); isLeader {
		if kv.nextConfig.Num == kv.config.Num { //如果已经同步到最新了，去寻求最新的
			//请求master的client 来得到config
			config := kv.mck.Query(kv.nextConfig.Num + 1)
			kv.startConfig(&config) //然后开始处理config
		}
	}
}
func (kv *ShardKV) startConfig(config *shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//如果可以被update 并且目前的是最新的
	if config.Num > kv.nextConfig.Num && kv.nextConfig.Num == kv.config.Num {
		op := Op{
			Command: *config,                  //请求数据
			Ch:      make(chan (interface{})), //日志提交chan
		}
		kv.rf.Start(op) //写入Raft  开始同步   所有的follower都含有这个config
	}
}

//raft commit后 所有业务的处理
func (kv *ShardKV) onApply(applyMsg raft.ApplyMsg) {

	//判断是否需要消减log
	if applyMsg.UseSnapshot {
		var LastIncludedIndex int
		var LastIncludedTerm int
		r := bytes.NewBuffer(applyMsg.Snapshot)
		d := gob.NewDecoder(r)
		//get basic info
		d.Decode(&LastIncludedIndex)
		d.Decode(&LastIncludedTerm)
		//initialize
		for i := 0; i < len(kv.kvs); i++ {
			kv.kvs[i] = make(map[string]string)
		}
		kv.notReadyShards = make(map[int][]int)
		//decode
		if d.Decode(&kv.reqIDs) != nil ||
			d.Decode(&kv.kvs) != nil ||
			d.Decode(&kv.config) != nil ||
			d.Decode(&kv.nextConfig) != nil ||
			d.Decode(&kv.notReadyShards) != nil {
		}
	} else {
		// kv.logApplyIndex = applyMsg.CommandIndex
		opt := applyMsg.Command.(Op)
		var resp interface{}
		if command, ok := opt.Command.(PutAppendArgs); ok {
			resp = kv.putAppend(&command)
		} else if command, ok := opt.Command.(GetArgs); ok { //Get操作
			resp = kv.get(&command)
		} else if command, ok := opt.Command.(shardmaster.Config); ok { //更新config
			kv.onConfig(&command)
		} else if command, ok := opt.Command.(ReqShards); ok {
			resp = kv.onGetShard(&command)
		} else if command, ok := opt.Command.(RespShards); ok {
			kv.onSetShard(&command)
		} else if command, ok := opt.Command.(RespNextConfig); ok {
			kv.onNextConfig(&command)
		} else if command, ok := opt.Command.(ReqDeleteShared); ok {
			kv.onDeleteShards(&command)
			//强制save数据
			if kv.maxraftstate != -1 {

				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				if e.Encode(&kv.reqIDs) != nil ||
					e.Encode(&kv.kvs) != nil ||
					e.Encode(&kv.config) != nil ||
					e.Encode(&kv.nextConfig) != nil ||
					e.Encode(&kv.notReadyShards) != nil {
				}
				data := w.Bytes()
				//开启一个snapshotting  消除自己的log
				go kv.rf.StartSnapshot(data, applyMsg.CommandIndex)
			}

		}
		select {
		case opt.Ch <- resp: //回复给server
		default:
		}

		//check length
		if kv.maxraftstate != -1 && kv.rf.GetPerisistSize() > kv.maxraftstate {
			w := new(bytes.Buffer)
			e := gob.NewEncoder(w)
			if e.Encode(&kv.reqIDs) != nil ||
				e.Encode(&kv.kvs) != nil ||
				e.Encode(&kv.config) != nil ||
				e.Encode(&kv.nextConfig) != nil ||
				e.Encode(&kv.notReadyShards) != nil {
			}
			data := w.Bytes()
			//开启一个snapshotting  消除自己的log
			go kv.rf.StartSnapshot(data, applyMsg.CommandIndex)
		}

	}

}

//apply 处理函数

//put
func (kv *ShardKV) putAppend(req *PutAppendArgs) Err {
	if !kv.isTrueGroup(req.Shard) {
		return ErrWrongGroup
	}
	if !kv.isRepeated(req.Me, req.ReqId) { //去重复
		if req.Op == "Put" {
			kv.kvs[req.Shard][req.Key] = req.Value
		} else if req.Op == "Append" {
			value, ok := kv.kvs[req.Shard][req.Key]
			if !ok {
				value = ""
			}
			value += req.Value
			kv.kvs[req.Shard][req.Key] = value
		}
	}
	return OK
}

//get
func (kv *ShardKV) get(req *GetArgs) (resp GetReply) {
	if !kv.isTrueGroup(req.Shard) {
		resp.Err = ErrWrongGroup
		return
	}
	value, ok := kv.kvs[req.Shard][req.Key]
	resp.WrongLeader = false
	resp.Err = OK
	if !ok {
		value = ""
		resp.Err = ErrNoKey
	}
	resp.Value = value
	return
}

//同步config  update 需要被更新的shards
func (kv *ShardKV) onConfig(config *shardmaster.Config) {
	if config.Num > kv.nextConfig.Num {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.nextConfig = *config
		kv.notReadyShards, _ = kv.getNewShards() //得到nextConfig里面的目前没有的shards
	}
}

//得到请求shard 复制自己的shard 返回
func (kv *ShardKV) onGetShard(req *ReqShards) (resp RespShards) {
	if req.ConfigNum > kv.config.Num { //自己未获取最新数据
		resp.Successed = false
		kv.timer.Reset(0) //获取最新数据
		return
	}
	resp.Successed = true
	resp.Group = kv.gid
	//复制已处理消息
	resp.ReqIDs = make(map[int64]int64)
	for key, value := range kv.reqIDs {
		resp.ReqIDs[key] = value
	}
	//复制分片数据
	resp.Data = make(map[int]map[string]string)
	for i := 0; i < len(req.Shards); i++ {
		shard := req.Shards[i]
		data := kv.kvs[shard]
		shardDatas := make(map[string]string)
		for key, value := range data {
			shardDatas[key] = value
		}
		resp.Data[shard] = shardDatas
	}
	return
}

//得到了数据 开始写入。  删除老group
func (kv *ShardKV) onSetShard(resp *RespShards) {
	kv.mu.Lock()
	_, ok := kv.notReadyShards[resp.Group]
	if !ok {
		return
	}
	delete(kv.notReadyShards, resp.Group) //删除这个组的需要的shards
	//更新数据
	for shard, kvs := range resp.Data {
		for key, data := range kvs {
			kv.kvs[shard][key] = data
		}
	}
	for key, value := range resp.ReqIDs {
		id, ok := kv.reqIDs[key]
		if !ok || id < value {
			kv.reqIDs[key] = value //跟新这个client的最新请求
		}
	}
	preConfig := kv.config
	kv.mu.Unlock()
	go kv.deleteGroupShards(&preConfig, resp) //开始删除别的组交出的shards
}

//更新config到下一个
func (kv *ShardKV) onNextConfig(resp *RespNextConfig) {
	if kv.cofigCompleted(resp.ConfigNum) { //数据已更新
		return
	}
	kv.mu.Lock()
	kv.config = kv.nextConfig
	// fmt.Println(kv.gid,kv.me,"on set config :",kv.config.Num)
	kv.mu.Unlock()
}

//删除shards
func (kv *ShardKV) onDeleteShards(req *ReqDeleteShared) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num == req.ConfigNum || kv.config.Num == req.ConfigNum+1 {
		info := ""
		for i := 0; i < len(req.Shards); i++ {
			shard := req.Shards[i]
			kv.kvs[shard] = make(map[string]string) //设置为空
			info += strconv.Itoa(shard)
			info += " "
		}
		fmt.Println(kv.gid, kv.me, "on delete", kv.config.Num, " config shards", info)
	}
}

////////////////// 主循环结束  //////////////////////////////////////////////////////////////

func (kv *ShardKV) shardLoop() { //nextConfig的变化，开始变换shards  然后顺利过渡到下一个config
	for !kv.killed {
		if !kv.isLeader() { //只要leader
			time.Sleep(time.Millisecond * 50)
			continue
		}
		isUpdated, shards := kv.isUpdateConfig() //查看是否需要update shards
		if !isUpdated {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		//需要update
		config := kv.nextConfig
		Num := config.Num //新的config的num
		waitCh := make(chan bool, len(shards))
		for key, value := range shards { //每个gid对应的shards
			go kv.getShardFromOther(key, value, waitCh, Num)
		}
		for !kv.killed && kv.isLeader() {
			if kv.cofigCompleted(Num) {
				break
			}
			if kv.isReadyShards(-1) {
				break
			}
			select {
			case <-waitCh:
			case <-time.After(time.Millisecond * 500): //超时
			}
		}
		if !kv.isReadyShards(-1) {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		//过渡到下一个config
		for !(kv.cofigCompleted(Num)) && !kv.killed && kv.isLeader() {
			respNextConfig := RespNextConfig{
				ConfigNum: Num,
			}
			kv.startNext(&respNextConfig)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

//从别的group 得到shards
func (kv *ShardKV) getShardFromOther(group int, shards []int, ch chan bool, num int) {
	defer func() { ch <- true }() //最后返回消息
	complete := false
	var resp RespShards
	for !complete && !(kv.cofigCompleted(num)) && !kv.killed {
		servers, ok := kv.config.Groups[group] //获取目标组服务
		if !ok {                               //等待 说明config还没有被update
			time.Sleep(time.Millisecond * 500)
			continue
		}
		req := ReqShards{
			ConfigNum: kv.nextConfig.Num,
			Shards:    shards,
		}
		for i := 0; i < len(servers); i++ {
			server := kv.make_end(servers[i])
			ok := server.Call("ShardKV.GetShard", &req, &resp)
			if ok && resp.Successed {
				complete = true
				break
			}
			if kv.cofigCompleted(num) || kv.killed {
				break
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
	//存储该分片数据
	for !kv.killed && !kv.cofigCompleted(num) && !kv.isReadyShards(group) && kv.isLeader() {
		successed, _ := kv.opt(resp) //发送rpc开始修改数据
		if successed {
			break
		}
	}
}

//修改config为下一个nextConfig
func (kv *ShardKV) startNext(shards *RespNextConfig) {
	kv.mu.Lock()
	if kv.config.Num < kv.nextConfig.Num {
		kv.mu.Unlock()
		op := Op{
			Command: *shards,                     //请求数据
			Ch:      make(chan (interface{}), 1), //日志提交chan
		}
		_, _, isLeader := kv.rf.Start(op) //写入Raft
		if isLeader {
			select {
			case <-op.Ch:
				// fmt.Println(kv.gid,kv.me,"next config success: ", kv.nextConfig.Num,"term",term,"index",index)
			case <-time.After(time.Millisecond * 1000): //超时
				// fmt.Println(kv.gid,kv.me,"next config timeout: ", kv.nextConfig.Num,"term",term,"index",index)
			}
		}
	} else {
		kv.mu.Unlock()
	}
}

////////////////// helper functions  /////////////////////////////////////////////////

func (kv *ShardKV) isUpdateConfig() (bool, map[int][]int) { //是否需要update 然后返回每个gid的 没准备好的shards
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rst := make(map[int][]int)
	for key, value := range kv.notReadyShards { //返回没有准备好的shard的copy
		rst[key] = value
	}
	return kv.nextConfig.Num > kv.config.Num, rst //判断下一个config的num是否大于 目前的
}

func (kv *ShardKV) cofigCompleted(Num int) bool { //这个config是否被完成
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num >= Num
}

//判断是否还有notReadyShards
func (kv *ShardKV) isReadyShards(group int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if group == -1 { //全局判断
		return len(kv.notReadyShards) == 0
	}
	_, ok := kv.notReadyShards[group] //单体判断
	return !ok
}

func (kv *ShardKV) isTrueGroup(shard int) bool { //给一个shard 然后判断这个kv有没有含有这个shard。或者已经有了，但是config还没update。
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num == 0 {
		return false
	}
	group := kv.config.Shards[shard]
	if group == kv.gid {
		return true
	} else if kv.nextConfig.Shards[shard] == kv.gid { //正在转移数据过程中
		_, ok := kv.notReadyShards[group] //改区的数据有没有转移过来
		return !ok
	} else {
		return false
	}
}

func (kv *ShardKV) isRepeated(client int64, msgId int64) bool { //RPC重复检测
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rst := false
	index, ok := kv.reqIDs[client]
	if ok {
		rst = index >= msgId
	}
	kv.reqIDs[client] = msgId
	return rst
}

func (kv *ShardKV) getNewShards() (map[int][]int, bool) {
	shards := make(map[int][]int)
	if kv.nextConfig.Num > kv.config.Num {
		if kv.nextConfig.Num > 1 {
			oldShards := GetGroupShards(&(kv.config.Shards), kv.gid)     //旧该组分片
			newShards := GetGroupShards(&(kv.nextConfig.Shards), kv.gid) //新该组分片
			for key, _ := range newShards {                              //获取新增组
				_, ok := oldShards[key]
				if !ok {
					group := kv.config.Shards[key]
					value, ok := shards[group]
					if !ok {
						value = make([]int, 0)
					}
					value = append(value, key)
					shards[group] = value
				}
			}
		}
		return shards, true
	}
	return shards, false
}

func (kv *ShardKV) deleteGroupShards(config *shardmaster.Config, respShard *RespShards) {
	req := ReqDeleteShared{
		ConfigNum: config.Num + 1,
	}
	for shard, _ := range respShard.Data {
		req.Shards = append(req.Shards, shard)
	}
	servers, ok := config.Groups[respShard.Group] //获取目标组服务
	if !ok {
		return
	}
	resp := RespDeleteShared{}
	for i := 0; i < len(servers); i++ {
		server := kv.make_end(servers[i])
		server.Call("ShardKV.DeleteShards", &req, &resp)
	}
}
