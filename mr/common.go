package mr

import (
	"fmt"
	"time"
)

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)

//taskPhase
const (
	MapPhase    int = 0
	ReducePhase int = 1
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

type Task struct {
	FileName string
	NReduce  int //id
	NMaps    int //id
	Seq      int
	Phase    int
	Alive    bool // worker should exit when alive is false
}

type TaskStat struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
