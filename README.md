# my-raft

6.824

Record the important points

lab2:

When making raft, the for loop inside the go program is opened indefinitely to detect changes in chan and react. For example, follower detects heartbeat and is ready to become a candidate at any time; leader sends heartbeat; candidate sends vote to check whether it becomes leader

Each term is initiated by election. Initiated by candidate, use rpc to synchronize other raft terms

appendEntries, only send the log after prev Index. If it is empty, then it is a heartbeat

All servers need to commit. When the follower detects that the leader has committed, he will commit. 1. Modify the state machine and apply apply to the server to modify the parameters and keep in sync. If the leader is down, become the leader. 2. When the 1/2 follower replica becomes. The leader will commit, and only commit the current term

lab3:

3a:

Kv server process sequence: clerk sends the request -> server accepts, calls start to add op to log -> raft learned that the leader adjusts the synchronization problem and then starts appendEntries when committing (applyCh has content). -> The server senses it and starts to process the content. -> server completes the task and reply to clerk

3b:

1. Server logic: the server detects that the persist content of raft is too long, StartSnapShot shortens the log itself

2. Initialization logic: raft is initialized -> get snapshot data of persist readSnapShot -> 1 check log whether it needs to be truncated. 2
   chanApply adds special msg -> server side decode to get index, term, db, ack, and load the last persist data

3. Leader's synchronization logic for follower: starting from appendEntries, if your own base is larger than others' next, it means that you have log compacted, then use snapshot to synchronize -> the receiver installs snap and modifies your log. chanApply adds special msg updates
   At the same time, reply also involves the degeneration of the leader and the function of updating the nextIndex of the follower.

lab4a:

1 The server receives the raft message, and then parses msg to obtain op. Then do different processing according to different businesses. For example, put and move require operations (lab4 requires operations). get only needs to add reply outside the main coroutine

2 Join requires each group to join the server. If it is a group that did not exist before, you need to readjust the slices of each group, and you need to take from the most groups until the difference with the most groups is less than or equal to 1.

3 The raft situation, such as the allocation of shards and the group itself are not forced, they are all recorded by config

4 Summary In lab4a, master is a shards management system, and it needs to ensure disaster tolerance. Groups and shards responsible for managing the system, load balancing

lab4b:

Handle disaster recovery in each group and realize the basic functions of kv server. When the master adjusts different groups and corresponding shards, kvshard sends shards to other groups through rpc. The synchronization problem of members in the group is similar to that of kvRaft.

1. The servers in a group are required to synchronize the migration data, because the server itself has a shard, which is part of the database.

High concurrent thinking:

1. Because different coroutines may enter a piece of code at the same time, for example, if you read and write to rf.age at the same time, there will be synchronization problems. Or in select, if you get two msgs at the same time, then there will be problems with the operation of rf.
2. In other words, only the variables shared by different coroutines need to be locked, such as operating class variables or operating global variables within a function of a class.
3. Pay attention to go func If there is a for loop outside, then many coroutines are started. Even if there is no for, at least two, because it is parallel to the main function

The respective functions of the server and raft:

1. Server: Accept the client's request, send it to raft through start, and wait for a period of time. After getting the apply of raft, get the content of kv database specifically, modify the content, and reply to the client
2. raft: After receiving the server command, you need to appendEntries to all the followers. When more than 1/2 of the members successfully append, commit, and reply to the server. appendEntries has its own consistency attribute.

todo:
Modify the role according to lab4

6.824

记录一下觉得重要的点

lab2:

make raft 的时候，go 协程序内部的 for 循环无限开启，检测 chan 的变化，做出反应。比如 follower，检测心跳，随时准备成为 candidate;leader 发送心跳; candidate 发送投票，检测是否变为 leader

每个 term 都由于选举发起。由 candidate 发起，通过 rpc 来同步其他 raft 的 term

appendEntries, 只发送 prev Index 后面的 log。如果是空，那么是心跳

所有的 server 都需要 commit，当 follower 检测到了 leader 已经 commit 了，他就会 commit。 1.修改状态机器，apply 传递给 server，来修改参数,保持同步，如果 leader down 了，成为 leader 2.当 1 / 2 的 follower replica 了。leader 才会 commit，并只会 commit 当前 term 的

lab3:

3a:

kv server 流程顺序： clerk 发送请求 -> server 接受，调用 start 加入 op 到 log -> raft 得知, leader 调整同步问题 然后开始 appendEntries 当 commit 的时候（applyCh 有内容了）。 -> server 感知到了 开始 处理内容。 -> server 完成任务 回复给 clerk

3b:

1.server 逻辑：server 监测到 raft 的 persist 内容太长，StartSnapShot 自己缩短 log

2.初始化逻辑： raft 被初始化 -> 得到 persist 的 snapshot data readSnapShot -> 1 check log 是否需要截断。 2
chanApply 加入特殊的 msg -> server 端 decode 得到 index, term, db, ack, 装载上次 persist 的数据

3.leader 对于 follower 的同步逻辑：从 appendEntries 出发， 如果自己的 base 都比别人的 next 大，说明自己 log compact 过，那么使用 snapshot 来同步 -> 接收者安装 snap，修改自己的 log。 chanApply 加入特殊的 msg 更新内容
同时 reply 还涉及到 leader 退化和更新 follower 的 nextIndex 的功能。

lab4a:

1 server 收到 raft 的消息，然后解析 msg 得到 op。 然后根据不同的业务做不同的处理。 比如 put 和 move 需要操作(lab4 都需要操作)。 get 只需要在主协程外的地方加入 reply 即可

2 join 需要每个 group 加入 server。如果是之前没有的组，还需要重新调整每个组的分片，需要从最多的组拿取，直到和最多组的差小于等于 1.

3 raft 的情况，比如 shards 的分配和 group 自己没逼数，都是靠 config 来记录。 这个和之前的 k - v 和 log 也是一样的

4 总结 lab4a，master 是一个 shards 的管理系统，自身需要保证容灾。负责管理系统的 groups 和负责的 shards，保证负载均衡

lab4b:

处理每个 group 内的容灾，实现基本的 kv server 的功能。当 master 调整 不同的分组和对应的 shards 时，kvshard 通过 rpc 发送 shard 到别的 group。组内成员的同步性问题和 kvRaft 类似。

1. 需要一个组内的 servers 同步迁徙数据，因为本身 server 带有 shard，即部分的数据库。

2. 当 config 发生改变时，需要 server 对于新拥有的 shards 做出处理。

高并发思考：

1. 因为不同的协程可能同时进入一段代码，比如：如果同时对于 rf.age 进行读写操作，那么会有同步性的问题。或者在 select 内部，如果同时得到了两个 msg，那么对于 rf 的操作也会出现问题。
2. 也就是说 只有操作不同协程共享的变量才需要加锁，比如一个类的函数内部操作类变量或者操作全局变量。
3. 注意 go func 如果外部有 for 循环，那么就启动了很多协程。即使没有 for，至少是两个，因为和主函数并行

server 端和 raft 的各自功能:

1. server： 接受 client 的请求，通过 start 发送给 raft，并等待一段时间。 得到 raft 的 apply 后，具体来得到 kv database 的内容，修改内容，回复给 client
2. raft： 接收到 server 端指令，需要 appendEntries 到所有的 follower，当超过 1/2 的成员成功 append 了，commit，回复给 server。appendEntries 自带 consistency 属性。

todo:
根据 lab4 修改作用
