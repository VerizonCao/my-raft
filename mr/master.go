package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	files     []string
	nReduce   int
	taskPhase int
	taskStats []TaskStat
	mu        sync.Mutex
	done      bool
	workerSeq int //目前分配到第几个worker
	taskCh    chan Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
// 查看job是否完成
func (m *Master) Done() bool {
	//这tm也要加锁吗
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}
	finish := true

	for index, t := range m.taskStats {
		switch t.Status {
		case TaskStatusReady:
			finish = false
			m.taskCh <- m.getTask(index)                //实际上是create了一个task
			m.taskStats[index].Status = TaskStatusQueue //进入队列

		case TaskStatusQueue:
			finish = false
		case TaskStatusRunning:
			finish = false
			//如果超时，重新放入队列
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.taskStats[index].Status = TaskStatusQueue
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinish:
			//出问题 也是放到queue中
		case TaskStatusErr:
			finish = false
			m.taskStats[index].Status = TaskStatusQueue
			m.taskCh <- m.getTask(index)
		}

	}
	if finish {
		if m.taskPhase == MapPhase {
			//把状态转为reduce
			m.initReduceTask()
		} else {
			m.done = true
		}

	}
}

func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,    //这个没意义
		NMaps:    len(m.files), //这个也没啥意义
		Seq:      taskSeq,      //这个是序号
		Phase:    m.taskPhase,  //这个是状态  map 还是 reduce
		Alive:    true,         //激活
	}
	// fmt.Print("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", m, taskSeq, len(m.files), len(m.taskStats))
	//如果是map  可以有file name
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.taskPhase = MapPhase
	m.files = files
	if nReduce < len(files) {
		m.taskCh = make(chan Task, len(files))
	} else {
		m.taskCh = make(chan Task, nReduce)
	}
	m.initMapTask()
	go m.tickSchedule() //这里用了go  因为需要下面的server也启动
	m.server()
	return &m
}

func (m *Master) initReduceTask() {
	// fmt.Println("init ReduceTask")
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

func (m *Master) initMapTask() {
	// fmt.Println("init MapTask")
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files)) //初始化 这个
}

func (m *Master) tickSchedule() { //选择一个schedule
	//无限循环  知道结束
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval) //休息一下
	}
}

//RPC receiver

//register worker
func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerSeq += 1
	reply.WorkerId = m.workerSeq
	return nil
}

//get the task situation, and make a schedule
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// fmt.Println("get report task: %+v, taskPhase: %+v", args, m.taskPhase)

	//如果不配套
	if m.taskPhase != args.Phase || args.WorkerId != m.taskStats[args.Seq].WorkerId {
		return nil
	}

	//改变状态
	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusErr
	}

	//马上schedule一次
	go m.schedule()
	return nil

}

//want to get atask
func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskCh
	reply.Task = &task

	if task.Alive {
		m.regTask(args, &task)
	}
	// fmt.Println("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.taskPhase {
		panic("req Task phase neq")
	}

	//设置开始时间  工人id  和  状态改变
	m.taskStats[task.Seq].Status = TaskStatusRunning
	m.taskStats[task.Seq].WorkerId = args.WorkerId
	m.taskStats[task.Seq].StartTime = time.Now()
}
