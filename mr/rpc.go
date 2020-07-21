package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
}

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Task *Task
}

type ReportTaskArgs struct {
	Done     bool
	Seq      int
	Phase    int
	WorkerId int
}

type ReportTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
