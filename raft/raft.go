package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

type NodeState uint8

const (
	Follower  = NodeState(1)
	Candidate = NodeState(2)
	Leader    = NodeState(3)
)

const (
	// HeartbeatInterval    = time.Duration(120) * time.Millisecond
	// ElectionTimeoutLower = time.Duration(300) * time.Millisecond
	// ElectionTimeoutUpper = time.Duration(400) * time.Millisecond
	HBINTERVAL = 50 * time.Millisecond // 50ms
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that Successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	//lab3
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}


//2A   log entry
type LogEntry struct {
	LogIndex int
	LogTerm  int
	LogComd  interface{} //command的内容
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers   貌似是RPC的接受端口
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A 2B
	state       NodeState
	currentTerm int
	log         []LogEntry // log Entries
	votedFor    int        //the server I vote for

	//volatile state on all servers
	commitIndex int //被commit的最last的log的index
	lastApplied int //被运用到state machine的 last log index
	//二者区别：  先commit 但是还没有回复给client

	//volatile state on leader
	NextIndex  []int //该server的下一个
	matchIndex []int //该server成功复制的最后一个
	voteCount  int   //被投票的个数

	//channel
	chanLeader    chan bool //我变成leader
	chanGrantVote chan bool //我投了票
	chanHeartbeat chan bool //我收到了脉冲
	chanCommit    chan bool //leader：有新的log commit了 但还没有 applied
	chanApply     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}

//输出一些信息
func (rf Raft) String() string {
	return fmt.Sprintf("[node(%d), state(%v), term(%d)]",
		rf.me, rf.state, rf.currentTerm)
}

//get the index and term of the last log
func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	//先加密再存储
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)

}


func (rf *Raft) readSnapshot(data []byte) {

	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex

	rf.log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.log)


	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}

	//应用snapshot到server
	go func() {
		rf.chanApply <- msg
	}()

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	//2A
	CandidateIndex int
	//用于验证   leader自身的属性
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {

	// Your data here (2A).
	Term        int  //用来更新leader的term，如果该leader 落伍了
	VoteGranted bool //投票
}

//example AppendEntries RPC request structure
type AppendEntriesArgs struct {

	//2A
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//example AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//可能同时受到两个request vote 全程加锁

	// Your code here (2A, 2B).
	//2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//2C
	defer rf.persist()

	reply.VoteGranted = false

	//如果请求的term小于我的term 修改  返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	//如果他的比我的大 我直接失去candidate身份
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term //update

		//退为follower  重置 便于下次投票
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm //返回term

	// last log's term and index
	term := rf.getLastTerm()
	index := rf.getLastIndex()

	uptoDate := false

	//看看是否需要更新  log
	if args.LastLogTerm > term {
		uptoDate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index { // at least up to date
		uptoDate = true
	}
	//根据规则2  如果没人投，或者 我投了他  并且candidate的信息至少updated to me
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateIndex) && uptoDate {
		rf.chanGrantVote <- true //投了票  表示不需要election计时，或者打断一次计时
		rf.state = Follower      //肯定是follower其实，不然不是CandidateIndex
		reply.VoteGranted = true
		rf.votedFor = args.CandidateIndex
	} else {
	}

}

//
// example AppendEntries RPC handler.
//

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//1 判断term的情况  然后看看是否需要作出修改
	//2 判断index
	//3 判断具体log的term和index

	//2A  只是heartBeat
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	//  如果不符合要求 直接返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1 //返回自己的last index， 供leader参考
		return
	}

	//至此，起码接收了heartBeat
	rf.chanHeartbeat <- true

	//比你大  candidate退化  和vote RPC 一样
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term //update
		//退为follower  重置 便于下次投票
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Term = args.Term //双方term 一样

	//比我的大 说明出现consistency问题，返回实际上我的下一个index
	if args.PrevLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	//如果index没问题  说明 leader 认为的follower的最后的匹配的index是对的

	baseIndex := rf.log[0].LogIndex
	if args.PrevLogIndex > baseIndex { //check bounds  如果base 是 0， 那根本不用管
		term := rf.log[args.PrevLogIndex-baseIndex].LogTerm //rf 的 最后一个index的 term
		//检查这个是否相等
		if args.PrevLogTerm != term {
			//term不相等 那么 从后往前  找到第一个term不相等的
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				//符合 那么返回下一个，也就是第一个出现问题的log的index
				if rf.log[i-baseIndex].LogTerm != term { //这里默认了 只有 term相等的才会出问题
					reply.NextIndex = i + 1 //出现问题的第一个
					break
				}
			}
			return
		}
	}

	//到了这里，说明index没问题，那两个index的term也相等

	if args.PrevLogIndex < baseIndex { //check bounds

	} else { //开始把新的log安装上去
		rf.log = rf.log[:args.PrevLogIndex+1-baseIndex] //前半部分一致的  自己的后面的舍弃了
		rf.log = append(rf.log, args.Entries...)        //新的部分
		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	}

	//rule 5  修改commitindex为leader的commitIndex和log最后面的index的较小者   意思是 follower得知 leader已经apply了，把自己的commit并且apply
	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true //有新的commit 需要跟新reply    如果去掉 2A test 过不了
		//这里说明了follower也需要commit，当他检测到了leader已经commit了，他就会commit
		//他commit的意义何在？修改状态机器，apply传递给server，来修改参数,保持同步。一方面是lab4实现的分布式，一方面leader down了，成为leader
	}
	return

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//  call 哪个  server
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//这个server来call RequestVote RPC
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {

		//我不是candidate  可能被降级了
		if rf.state != Candidate {
			return ok //无需操作 直接返回
		}
		//有bug
		if args.Term != rf.currentTerm {
			return ok
		}
		//比自己先进 降级
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term

			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
		}
		//follower投了自己
		if reply.VoteGranted {
			rf.voteCount++
			//如果得到了大部分的选票
			if rf.state == Candidate && rf.voteCount > len(rf.peers)/2 {
				rf.state = Leader //我修改了这里
				rf.chanLeader <- true
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//发送AppendEntries
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		//check conditions
		if rf.state != Leader {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}

		//更新leader专有的信息

		if reply.Success {
			//表示follower回复自己，没有问题，已经搞定
			if len(args.Entries) > 0 {
				//该server的下一个index是 entry最后一个元素的index + 1
				rf.NextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
				//server的matchIndex是该server的NextIndex - 1   这个也make sense
				rf.matchIndex[server] = rf.NextIndex[server] - 1
			}
		} else {
			//consistency问题   这个server的NextIndex变为 reply的NextIndex
			rf.NextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

//readPersistenceSize  getStateSize
func (rf *Raft) GetPerisistSize() int {
	return rf.persister.RaftStateSize()
}


//snapshotting
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte    //具体的snapshot内容

	//not used
	Offset           int
	Done             bool
}

type InstallSnapshotReply struct {
	Term int
}


//自己监测到log太多，发动的snapshot制作新的log entries  然后存储 state 和 snapshot
func (rf *Raft) StartSnapshot(snapshot []byte, index int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].LogIndex
	lastIndex := rf.getLastIndex()

	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	var newLogEntries []LogEntry

	newLogEntries = append(newLogEntries, LogEntry{LogIndex: index, LogTerm: rf.log[index-baseIndex].LogTerm})

	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.log[i-baseIndex])
	}

	rf.log = newLogEntries

	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].LogIndex)
	e.Encode(newLogEntries[0].LogTerm)

	data := w.Bytes()
	data = append(data, snapshot...)  
	rf.persister.SaveSnapshot(data)


}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []LogEntry) []LogEntry {

	var newLogEntries []LogEntry
	//设置这个为空
	newLogEntries = append(newLogEntries, LogEntry{LogIndex: lastIncludedIndex, LogTerm: lastIncludedTerm})


	//从后往前
	for index := len(log) - 1; index >= 0; index-- {
		//如果这个log的 index 和 term 和 传入的 最后被加入snap的那个log 匹配
		if log[index].LogIndex == lastIncludedIndex && log[index].LogTerm == lastIncludedTerm {
			//加入 这个除外的之后的所有.
			newLogEntries = append(newLogEntries, log[index+1:]...)
			break
		}
	}

	return newLogEntries
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs,reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.chanHeartbeat <- true
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1    //add by me

	rf.persister.SaveSnapshot(args.Data)   //存储 snapshot

	rf.log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()
	//应用snapshot到server
	rf.chanApply <- msg

}


//发送 snapShot 的命令  让 follower 来操作
func (rf *Raft) sendInstallSnapshot(server int,args InstallSnapshotArgs,reply *InstallSnapshotReply) bool {

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			return ok
		}

		rf.NextIndex[server] = args.LastIncludedIndex + 1  
		rf.matchIndex[server] = args.LastIncludedIndex  //修改为 成功 存入snap的最后的index
	}
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false.
// otherwise start the agreement and return immediately.

// there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//2A  2B
	index := -1
	term := rf.currentTerm
	isLeader := rf.isLeader()

	if isLeader { //如果是leader 马上存入log里面，但是没确认
		index = rf.getLastIndex() + 1
		//存入
		rf.log = append(rf.log, LogEntry{LogTerm: term, LogComd: command, LogIndex: index})
		rf.persist()
	}


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
//

func (rf *Raft) boatcastRequestVote() {
	//给每个server发送信息

	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:           rf.currentTerm,
		CandidateIndex: rf.me,
		LastLogIndex:   rf.getLastIndex(),
		LastLogTerm:    rf.getLastTerm(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate { //是candidate并且发送的不是自己
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, &args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) boatcastAppendEntries() {
	// 1 check是否需要commit更多的log
	// 2 给每个server发送heartBeat或appendEntries

	rf.mu.Lock()
	defer rf.mu.Unlock()

	N := rf.commitIndex
	last := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex

	//从commit的到最后
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			//server的这个已经commit   并且    term符合
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].LogTerm == rf.currentTerm {
				num++
			}
		}
		//如果超过一半的，那么commit这个log
		if 2*num > len(rf.peers) {
			N = i
		}
	}

	//有所突破和更新，需要新的commit
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- true //提醒需要respond to client
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {

			//如果这个follower的下一个 比我的base大。
			if rf.NextIndex[i] > baseIndex {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,               //add by me
					PrevLogIndex: rf.NextIndex[i] - 1, //实质是leader认为的这个server的最后元素的index  为啥不是leader的最新的呢（lastIndex）？ 因为需要慢慢和folloewer保持一致
				}
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].LogTerm
				//entry 认为的 follower的没有的 元素
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1-baseIndex:])) //还是担心不是从0开始
				copy(args.Entries, rf.log[args.PrevLogIndex+1-baseIndex:])                   //搬运过来
				args.LeaderCommit = rf.commitIndex
				//开始发送心跳
				go func(i int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(i, &args, &reply)
				}(i, args)
			} else {  //如果他的下一个比我的base还小。那么是我已经使用了snapshot 。需要发送snapshot.
				var args InstallSnapshotArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				//最后被包括进入snapshot的是log的第一个元素
				args.LastIncludedIndex = rf.log[0].LogIndex
				args.LastIncludedTerm = rf.log[0].LogTerm
				args.Data = rf.persister.snapshot
				go func(server int,args InstallSnapshotArgs) {
					reply := &InstallSnapshotReply{}
					rf.sendInstallSnapshot(server, args, reply)
				}(i,args)

			}
		}
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[].

// this server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 制造了一个新的server instance    需要立刻返回
// 每个server被制造 其中内嵌的go无限循环方法，就是相当于这个server一直在运行

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers //所有的peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	//2A
	rf.state = Follower
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0

	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanApply = applyCh

	// 2C，3B   initBialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go func() {
		//无限循环  等待
		for {
			switch rf.state {
			case Follower:
				select {
				//不需要update 投票
				case <-rf.chanHeartbeat: //之前成功接收过heartBeat
				case <-rf.chanGrantVote: //之前被要求投票
				//如果过了这么久 那么变成candidate
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
					rf.state = Candidate
				}

			case Leader:
				rf.boatcastAppendEntries() //发送心跳
				time.Sleep(HBINTERVAL)     //休息一段时间  不应期

			case Candidate:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				//2C
				rf.persist()
				rf.mu.Unlock()

				//发送投票
				go rf.boatcastRequestVote()

				select {
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
				case <-rf.chanHeartbeat:
					rf.state = Follower
				//成功变成leader

				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = Leader
					rf.NextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						//设置为自己的最后一个index + 1
						rf.NextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}

			}
		}
	}()

	//实时监测，所以用go开一个线程
	//如果leader有commit新的log  那么回复client
	go func() {
		for {
			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.log[0].LogIndex
				//从已经appied的到commit的   进行apply  看到这里貌似是  所有server都需要对于一个command 进行
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{CommandValid: true, CommandIndex: i, Command: rf.log[i-baseIndex].LogComd}
					applyCh <- msg
					rf.lastApplied = i //update
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
