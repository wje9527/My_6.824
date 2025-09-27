package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// coordinator的结构
	nReduce     int
	mu          sync.Mutex //锁
	files       []string   // 所有文件
	mapTasks    []TaskInfo // 所有maptask
	reduceTasks []TaskInfo // 所有reducetask
	mapdone     bool
	reducedone  bool
}

// Your code here -- RPC handlers for the worker to call.

type TaskState int

const (
	Idle TaskState = iota
	Inprogress
	Completed
)

const timeout = 10 * time.Second

type TaskInfo struct {
	Task      Task      // 任务结构
	Statu     TaskState //任务状态
	StartTime time.Time //任务开始时间
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 自定义的task 分发rpc
func (c *Coordinator) AssignTask(args *Request, reply *Task) error {
	// 一个文件一个worker
	// 对共享的coordinator上锁
	c.mu.Lock()
	defer c.mu.Unlock()

	// 遍历 mapTask组找到要分配的任务
	if !c.mapdone {
		for i := range c.mapTasks {

			if c.mapTasks[i].Statu == Idle ||
				(c.mapTasks[i].Statu == Inprogress && time.Since(c.mapTasks[i].StartTime) > timeout) {
				*reply = Task{Type: c.mapTasks[i].Task.Type,
					ID:        c.mapTasks[i].Task.ID,
					Filenames: c.mapTasks[i].Task.Filenames,
					NReduce:   c.mapTasks[i].Task.NReduce,
				}

				c.mapTasks[i].StartTime = time.Now()
				c.mapTasks[i].Statu = Inprogress
				return nil
			}

		}

		// 遍历完没有找到需要重新分配的任务，那就是都在计算中，需要告诉worker进行等待
		*reply = Task{Type: WaitTask}
		log.Println("所有map都分配完，等待处理结束")
		return nil
	}

	// 遍历reduceTask组找到要分配的reduce任务
	if !c.reducedone {
		for i := range c.reduceTasks {

			if c.reduceTasks[i].Statu == Idle ||
				(c.reduceTasks[i].Statu == Inprogress && time.Since(c.reduceTasks[i].StartTime) > timeout) {
				*reply = Task{Type: c.reduceTasks[i].Task.Type,
					ID:        c.reduceTasks[i].Task.ID,
					Filenames: c.reduceTasks[i].Task.Filenames,
					NReduce:   c.reduceTasks[i].Task.NReduce,
				}

				c.reduceTasks[i].StartTime = time.Now()
				c.reduceTasks[i].Statu = Inprogress
				return nil
			}

		}
		// 遍历完没有找到需要重新分配的任务，那就是都在计算中，需要告诉worker进行等待
		*reply = Task{Type: WaitTask}
		log.Println("所有reduce都分配完，等待处理结束")
		return nil
	}

	// 返回一个结束任务
	*reply = Task{Type: ExitTask}
	log.Println("所有任务已完成，worker没有任务")
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
	// 检查所有任务完成就是检查reducedone
	c.mu.Lock()
	defer c.mu.Unlock()

	// 返回reduce任务完成状态
	return c.reducedone
}

func (c *Coordinator) MarkTaskDone(task *DoneArgs, reply *DoneReply) error {
	// Your code here.
	// 锁定c的任务列表
	c.mu.Lock()
	defer c.mu.Unlock()

	// 把相应ID的任务状态置为Completed
	if task.Type == MapTask {
		c.mapTasks[task.ID].Statu = Completed
		allmapdone := true
		// 更新完检查完成状态
		for i := range c.mapTasks {
			if c.mapTasks[i].Statu != Completed {
				allmapdone = false
				break
			}
		}

		if allmapdone {
			c.mapdone = true
			log.Printf("所有map任务都完成了,准备开始reduce阶段")
		}
	} else if task.Type == ReduceTask {
		allreducedone := true
		c.reduceTasks[task.ID].Statu = Completed
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Statu != Completed {
				allreducedone = false
				break
			}
		}
		if allreducedone {
			c.reducedone = true
			log.Printf("所有reduce任务都完成了，任务结束")
		}
	}

	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 创建一个coordinator来管理分发的任务
	c.files = files
	c.nReduce = nReduce
	c.mapTasks = make([]TaskInfo, len(files))
	c.reduceTasks = make([]TaskInfo, nReduce)
	c.mapdone = false
	c.reducedone = false
	// 为maptask分配文件
	for i, file := range files {
		c.mapTasks[i] = TaskInfo{
			Task: Task{Type: MapTask,
				ID:        i,
				Filenames: []string{file},
				NReduce:   nReduce,
			},
			Statu: Idle,
		}
	}

	// 为reduce task初始化信息
	for i := 0; i < nReduce; i++ {
		intermediateFiles := []string{}
		for _, j := range c.mapTasks {
			filename := fmt.Sprintf("mr-%d-%d", j.Task.ID, i)
			intermediateFiles = append(intermediateFiles, filename)
		}
		c.reduceTasks[i] = TaskInfo{
			Task: Task{Type: ReduceTask,
				ID:        i,
				Filenames: intermediateFiles,
				NReduce:   nReduce,
			},
			Statu: Idle,
		}
	}
	c.server()
	return &c
}
