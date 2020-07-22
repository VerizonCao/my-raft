# my-raft
6.824






lab3:


3b:

自己逻辑：server监测到太长，StartSnapShot 自己缩短log

初始化逻辑： raft被初始化 -> 得到persist的snapshot data  readSnapShot ->  1 check log 截断。 2
chanApply 加入特殊的 msg  ->   server端 decode得到index, term, db, ack

和follower的同步逻辑：从appendEntries出发， 如果自己的base都比别人的next大，那么使用snapshot来同步 -> 接收者安装snap，修改自己的log。 同时还涉及到leader退化和更新follower的nextIndex的功能。