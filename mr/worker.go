package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//

//定义的两个string的结构体
type KeyValue struct {
	Key   string
	Value string
}

type worker struct { //id  map  reduce方法
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// 根据string 和函数来得到 task number 对于每个key-value pair
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
// 被mrWorker调用  这里只是启用了一个worker？ 或者多次调用mrWorker函数
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()

}

func (w *worker) run() {
	//处理任务  处理完一个处理下一个
	for {
		t := w.reqTask()
		if !t.Alive {
			fmt.Println("finish")
			return
		} else {
			w.doTask(t)
		}
	}
}

func (w *worker) doTask(t Task) {
	fmt.Println("in do Task")
	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase err: %v", t.Phase))
	}
}

func (w *worker) doMapTask(t Task) {
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}

	//3 步
	//1 根据file来得到kv pairs
	//2 根据key hash分配到不同的reducer内
	//3 对于每个reducer的任务，写入到文件名字为 taskId和reducerId内。

	kvs := w.mapf(t.FileName, string(contents))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := reduceName(t.Seq, idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}

		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)

}

//根据 file的id和seq，
func (w *worker) doReduceTask(t Task) {
	//结果放入 mr-out-X  有几个reduce就有几个out
	maps := make(map[string][]string)
	//对于每个map的结果
	for idx := 0; idx < t.NMaps; idx++ {
		//得到名字
		fileName := reduceName(idx, t.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			//得到kv
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			//存入kv
			if _, ok := maps[kv.Key]; !ok { //如果还没有这个key的string[]
				maps[kv.Key] = make([]string, 0, 200)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps { //每一个不同的key
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	//write out-file
	if err := ioutil.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}

	w.reportTask(t, true, nil)

}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//  如果得到false 说明 主程序结束了  那么我也结束
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) //剔除申请
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//report Task situation
func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Seq = t.Seq
	args.Phase = t.Phase
	args.WorkerId = w.id
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		fmt.Println("report task fail:%+v", args)
	}

}

//get a task
func (w *worker) reqTask() Task {
	args := TaskArgs{}
	args.WorkerId = w.id
	reply := TaskReply{}

	if ok := call("Master.GetOneTask", &args, &reply); !ok {
		os.Exit(1)
	}
	return *reply.Task
}

//register worker, get the worker id
func (w *worker) register() {

	args := RegisterArgs{}
	reply := RegisterReply{}
	if ok := call("Master.RegWorker", &args, &reply); !ok {
		log.Fatal("reg fail")
	}
	w.id = reply.WorkerId

}
