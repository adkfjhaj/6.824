package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct {
	//taskType TaskType
	//taskId int
	//filename string
	//state   State
}
type State int

const (
	Working State = iota
	Waiting
	Done
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
	WaitingTask
)

type TaskReply struct {
	TaskType  TaskType //哪怕是结构体中的字段名 若不大写 在没有引用这个包外的文件中无法访问 比如rpc相关的包
	TaskId    int
	FileSlice []string //文件切片 针对与map阶段保存一个文件 reduce阶段保存多个中间文件
	ReduceNum int
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the worker.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func workerSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
