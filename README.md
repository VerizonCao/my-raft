# my-raft

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

高并发思考：

1. 因为不同的协程可能同时进入一段代码，比如：如果同时对于 rf.age 进行读写操作，那么会有同步性的问题。或者在 select 内部，如果同时得到了两个 msg，那么对于 rf 的操作也会出现问题。
2. 也就是说 只有操作不同协程共享的变量才需要加锁，比如一个类的函数内部操作类变量或者操作全局变量。
3. 注意 go func 如果外部有 for 循环，那么就启动了很多协程。即使没有 for，至少是两个，因为和主函数并行

server 端和 raft 的各自功能:

1. server： 接受 client 的请求，通过 start 发送给 raft，并等待一段时间。 得到 raft 的 apply 后，具体来得到 kv database 的内容，修改内容，回复给 client
2. raft： 接收到 server 端指令，需要 appendEntries 到所有的 follower，当超过 1/2 的成员成功 append 了，commit，回复给 server。appendEntries 自带 consistency 属性。
