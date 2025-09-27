package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// work应该向coordinator询问任务
	for {
		// 创建一个replay，就是coordinator返回的一个任务
		request := Request{}
		reply := Task{} // Task 对象

		ok := call("Coordinator.AssignTask", &request, &reply)

		// 检查Coordinator 是否结束
		if !ok {
			fmt.Println("Coordinator is exited , Worker is exiting")
			break
		}

		switch reply.Type {
		case MapTask:
			// fmt.Println("Worker recived a Map task for file %s\n", replay.Filenames[0])
			log.Printf("Worker 收到map任务 #%d, 处理文件 %s", reply.ID, reply.Filenames[0])
			filename := reply.Filenames[0]
			content, err := os.ReadFile(filename)
			// 读取失败就跳过
			if err != nil {
				log.Printf("读取文件:%s,失败:%v", filename, err)
				continue
			}
			kva := mapf(reply.Filenames[0], string(content))
			// 把KV写入不同的文件
			buckets := make([][]KeyValue, reply.NReduce)
			// 准备好bucket
			for i := range buckets {
				buckets[i] = make([]KeyValue, 0)
			}

			//把kva 放入buckets中
			for _, kv := range kva {
				reduceTaskID := ihash(kv.Key) % reply.NReduce
				buckets[reduceTaskID] = append(buckets[reduceTaskID], kv)
			}

			// 写入中间文件
			for i := 0; i < reply.NReduce; i++ {
				// 分布式写入经典操作，先创建临时文件，成功之后再改名
				tempfile, err := os.CreateTemp("", "mr-temp-*")
				if err != nil {
					log.Printf("无法创建临时文件")
					return
				}
				enc := json.NewEncoder(tempfile)
				for _, kv := range buckets[i] {
					if err := enc.Encode(&kv); err != nil {
						log.Printf("写入json到文件 %s 失败：%v", tempfile.Name(), err)
					}
				}

				// 关闭临时文件
				if err := tempfile.Close(); err != nil {
					log.Printf("关闭文件 %s 失败:%v", tempfile.Name(), err)
				}

				// 重命名临时文件
				finalName := fmt.Sprintf("mr-%d-%d", reply.ID, i)
				if err := os.Rename(tempfile.Name(), finalName); err != nil {
					log.Printf("重命名文件%s 失败: %v", tempfile.Name(), err)
				}
			}

			// map任务完成之后通过rpc 向coordinator 汇报
			doneArgs := DoneArgs{Type: MapTask, ID: reply.ID}
			doneReply := DoneReply{}
			call("Coordinator.MarkTaskDone", &doneArgs, &doneReply)
			log.Printf("Worker done the maptask %d", doneArgs.ID)
		case ReduceTask:
			fmt.Println("Worker 收到一个reduce任务")
			// 遍历每一个文件，然后输出mr-out-*
			intermediateKV := []KeyValue{}
			for _, filename := range reply.Filenames {
				// content, err := os.ReadFile(file)
				file, err := os.Open(filename)
				if err != nil {
					log.Printf("reduce任务:%d 打开中间文件:%s 失败", reply.ID, filename)
					continue
				}
				// json解码
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediateKV = append(intermediateKV, kv)
				}
			}

			sort.Sort(ByKey(intermediateKV))
			otemp := fmt.Sprintf("mr-out-temp")
			tempfile, err := os.CreateTemp("", otemp)
			if err != nil {
				log.Printf("reduce 任务中的temp文件创建失败")
			}
			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediateKV) {
				j := i + 1
				for j < len(intermediateKV) && intermediateKV[j].Key == intermediateKV[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediateKV[k].Value)
				}
				output := reducef(intermediateKV[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempfile, "%v %v\n", intermediateKV[i].Key, output)

				i = j
			}
			tempfile.Close()

			finalName := fmt.Sprintf("mr-out-%d", reply.ID)
			if err := os.Rename(tempfile.Name(), finalName); err != nil {
				log.Printf("重命名文件%s 失败: %v", tempfile.Name(), err)
			}

			doneArgs := DoneArgs{
				Type: ReduceTask,
				ID:   reply.ID,
			}

			doneReply := DoneReply{}

			call("Coordinator.MarkTaskDone", &doneArgs, &doneReply)
			log.Printf("Worker done the reducetask %d", doneArgs.ID)

		case WaitTask:
			// coordinator 让worker来等待 可能所有map任务都分发完毕，但是reduce任务还没有准备好
			fmt.Println("Worker told to wait")
			time.Sleep(50 * time.Millisecond)
		case ExitTask:
			// coordinator 让worker退出，任务已经结束
			fmt.Println("Worker told to exit")
			return
		}

		// // 沉睡一段时间再次领取任务
		// time.Sleep(1 * time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
