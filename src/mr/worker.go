package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//func GetTask() TaskReply {
//
//	//wMu.Lock()
//	args := TaskArgs{}
//	reply := TaskReply{}
//	ok := call("Coordinator.AskforTask", &args, &reply)
//	//wMu.Unlock()
//	if ok {
//		//fmt.Println("worker get ", reply.TaskType, "task :Id[", reply.TaskId, "]")
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//	return reply
//
//}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	keepflag := true
	for keepflag {
		args := TaskArgs{}
		reply := TaskReply{}
		//reply := GetTask()
		ok := call("Coordinator.AskforTask", &args, &reply)
		if ok {
			fmt.Println(reply)
		} else {
			fmt.Println("AskforTask error!")
		}

		switch reply.TaskType {
		case MapTask:
			fmt.Println("Ask successful!---MapTask!")
			DoMap(mapf, &reply)
			CallDone(&reply)
		case ReduceTask:
			fmt.Println("MapTask allocate done!")
			fmt.Println("ReduceTask!")
			DoReduce(reducef, &reply)
			CallDone(&reply)
		case WaitingTask:
			fmt.Println("All task in progress, please wait!")
			time.Sleep(time.Second * 5)
		case ExitTask:
			fmt.Println("ExitTask!")
			keepflag = false
		}
	}
	time.Sleep(time.Second)

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}
func DoMap(mapf func(string, string) []KeyValue, reply *TaskReply) {
	intermediate := []KeyValue{}

	file, err := os.Open(reply.FileSlice[0])
	if err != nil {
		log.Fatal("cannot open %v", reply.FileSlice[0])
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %v", reply.FileSlice[0])
	}
	file.Close()
	intermediate = mapf(reply.FileSlice[0], string(content))
	//map 阶段应该将中间文件分成nReduce个桶按照在中间文件中的key
	HashedKV := make([][]KeyValue, reply.ReduceNum)
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % reply.ReduceNum
		HashedKV[idx] = append(HashedKV[idx], kv)
	}
	for i := 0; i < reply.ReduceNum; i++ {
		oname := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("encode error!")
			}
		}
		ofile.Close()
	}

}

func CallDone(task *TaskReply) {
	args := task
	reply := TaskReply{}
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if ok {
		fmt.Println("MarkFinished success!")
	} else {
		fmt.Println("MarkFinished failed!")
	}

}

// 将中间文件排序 并且返回形式为[]keyValue
func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}

	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("cannot open %v", filepath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func DoReduce(reducef func(string, []string) string, reply *TaskReply) {
	reduceFileNum := reply.TaskId
	intermediate := shuffle(reply.FileSlice)
	dir, _ := os.Getwd()
	tempfile, err := os.CreateTemp(dir, "mr-tmp-*")
	//oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	//ofile, err := os.Create(oname)

	if err != nil {
		log.Fatal("cannot create tempfile", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempfile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempfile.Name(), fn)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
