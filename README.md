# my-raft
6.824






lab3:


3b:

1.自己逻辑：server监测到太长，StartSnapShot 自己缩短log

2.初始化逻辑： raft被初始化 -> 得到persist的snapshot data  readSnapShot ->  1 check log 截断。 2
chanApply 加入特殊的 msg  ->   server端 decode得到index, term, db, ack

3.和follower的同步逻辑：从appendEntries出发， 如果自己的base都比别人的next大，那么使用snapshot来同步 -> 接收者安装snap，修改自己的log。 同时还涉及到leader退化和更新follower的nextIndex的功能。


高并发思考：

1. 因为不同的协程可能同时进入一段代码，如果同时对于rf.age 进行读写操作，那么会有同步性的问题。或者在select内部，如果同时得到了两个msg，那么对于rf的操作也会出现问题。
2. 也就是说 只有操作不同协程共享的变量才需要加锁，比如一个类的函数或者操作全局变量。
3. 注意 go func 因为如果外部有for循环，那么就启动了很多协程。至少是两个，因为和主函数并行