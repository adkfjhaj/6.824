package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int
	files             []string
	TaskId            int
	DistPhase         Phase //整个框架目前处于什么阶段
	TaskMapChannel    chan *TaskReply
	TaskReduceChannel chan *TaskReply
	taskMetaHolder    TaskMetaHolder
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo //int是key *TaskMetoInfo是value
}

type TaskMetaInfo struct {
	state     State
	TaskAdr   *TaskReply
	StartTime time.Time //记录这个task的开始时间 Hint中提示到记录一个task时间 超过某个预定时间认为此task已死
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTask()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      int
		mapUnDoneNum    int
		reduceDoneNum   int
		reduceUnDoneNum int
	)
	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	//fmt.Println("map task finishednum:", mapDoneNum/(mapUnDoneNum+mapDoneNum))
	//fmt.Println("reduce task finishednum:", reduceDoneNum/(reduceUnDoneNum+reduceDoneNum))
	//统计task完成情况 若完成当前阶段 则返回true切换为下一个阶段
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}
	return false
}

func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskMetaInfo, ok := t.MetaMap[taskId]
	if !ok || taskMetaInfo.state != Waiting {
		return false
	}
	//更改task元信息 定义开始时间
	taskMetaInfo.state = Working
	taskMetaInfo.StartTime = time.Now()
	return true
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskforTask(args *TaskArgs, reply *TaskReply) error {
	fmt.Println("AskforTask!")
	//1. channel不是天然线程安全吗？加锁是否有必要？ 有必要！这里是访问到了共享资源Coordinator 此时其他的worker会在其他地方使用到Cordinator
	//2. mapTask需要被分配 reduceTask也需要请求分配吗？ 需要！reduceTask还是由worker来完成

	//防止多个worker竞争
	mu.Lock()
	defer mu.Unlock()
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskMapChannel) > 0 {
				*reply = *<-c.TaskMapChannel
				//判断此时的task状态是否为waiting 并且将其更改为working
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Println("MapTask is running!", reply.TaskId)
				}
			} else {
				//map任务已经分配完成了 但是此刻正在执行map task 整个MR还在MapPhase未进入ReducePhase
				reply.TaskType = WaitingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskReduceChannel) > 0 {
				*reply = *<-c.TaskReduceChannel
				//判断此时的task状态是否为waiting 并且将其更改为working
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Println("ReduceTask is running!", reply.TaskId)
				}
			} else {
				reply.TaskType = WaitingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		panic("Unknown phase!")
	}
	return nil
}

// 在worker做完map 或者reduce后 调用callDone() 在这里修改保存在coordinator中的task状态
func (c *Coordinator) MarkFinished(args *TaskReply, reply *TaskReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		{
			//map结构会返回两个值 一个是健所对应的 另一个是请求的建是否在map中
			metaInfo, ok := c.taskMetaHolder.MetaMap[args.TaskId]
			if ok && metaInfo.state == Working {
				//fmt.Println("MapTask has finished!", args.TaskId)
				metaInfo.state = Done
			} else {
				fmt.Println("maptask has been finished!", args.TaskId)
			}
			break
		}
	case ReduceTask:
		{
			metaInfo, ok := c.taskMetaHolder.MetaMap[args.TaskId]
			if ok && metaInfo.state == Working {
				//fmt.Println("ReduceTask has finished!", args.TaskId)
				metaInfo.state = Done
			} else {
				fmt.Println("ReduceTask has been finished!", args.TaskId)
			}
			break
		}
	default:
		panic("Unknown task type!")
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Println("All task are finished!")
		return true
	}
	return false
}

func (c *Coordinator) generateTaskId() int {
	//获取当前已经分配的taskId
	//然后自增
	taskId := c.TaskId
	c.TaskId++
	return taskId
}

// 传进去参数i就是中间文件的最后一个数字来确定 当前任务处理的是那一块中间文件
func selectReduceNum(num int) []string {
	s := []string{}
	path, _ := os.Getwd()
	files, _ := os.ReadDir(path)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-") && strings.HasSuffix(f.Name(), strconv.Itoa(num)) {
			s = append(s, f.Name())
		}
	}
	return s
}

func (c *Coordinator) makeReduceTask() {
	for i := 0; i < c.ReducerNum; i++ {
		t := TaskReply{
			TaskId:    c.generateTaskId(),
			TaskType:  ReduceTask,
			FileSlice: selectReduceNum(i),
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &t,
		}
		c.taskMetaHolder.MetaMap[t.TaskId] = &taskMetaInfo
		fmt.Println("make a Reduce task:", &t)
		c.TaskReduceChannel <- &t
	}
}

// 将maptask放入管道中，并且记录task的meta数据
func (c *Coordinator) makeMapTask(files []string) {
	for _, file := range files {
		t := TaskReply{
			TaskId:    c.generateTaskId(),
			TaskType:  MapTask,
			FileSlice: []string{file}, //与[]string(file)的不同在于{ }是使用file创建了一个切片 ()是转换成为一个切片
			ReduceNum: c.ReducerNum,
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &t,
		}
		//这里需要判断有重复的task meta info 吗？ 不需要！！
		c.taskMetaHolder.MetaMap[t.TaskId] = &taskMetaInfo
		fmt.Println("make a map task:", &t)
		//添加进管道
		c.TaskMapChannel <- &t
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		DistPhase:         MapPhase,
		files:             files,
		ReducerNum:        nReduce,
		TaskReduceChannel: make(chan *TaskReply, nReduce),
		TaskMapChannel:    make(chan *TaskReply, len(files)),
		taskMetaHolder: TaskMetaHolder{
			make(map[int]*TaskMetaInfo, len(files)+nReduce), //任务总数
		},
	}
	c.makeMapTask(files)

	c.server()
	//开启一个协程，用于检测任务是否崩溃 若是崩溃重新加入到taskchannel中去
	go c.CrashDetector()
	return &c
}
func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}
		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working && time.Since(v.StartTime) > time.Second*9 {
				fmt.Println("Task crash:", v.TaskAdr.TaskId, "-----", v.TaskAdr.TaskType)
				switch v.TaskAdr.TaskType {
				case MapTask:
					c.TaskMapChannel <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.TaskReduceChannel <- v.TaskAdr
					v.state = Waiting
				}
			}
		}
		mu.Unlock()
	}
}
