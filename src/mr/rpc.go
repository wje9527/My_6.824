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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

// 提前定义好任务的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// request的结构 是rpc的参数类型
type Request struct {
}

// task的结构
type Task struct {
	Type      TaskType
	ID        int      // task_id
	Filenames []string //包含map 和 reduce任务要处理的中间文件
	NReduce   int      // NReduce 用来计算任务ID的哈希
}

// 完成任务的回复结构
type DoneArgs struct {
	Type TaskType // 任务类型
	ID   int      // 完成任务的ID
}

type DoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
