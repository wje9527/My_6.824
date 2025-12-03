package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu          sync.Mutex
	KeyVersion  map[string]rpc.Tversion
	KeyValue    map[string]string
	lastRequest map[int64]int // 记录每个client的最后一个request
	lastReply   map[int64]any
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch request := req.(type) {
	case rpc.GetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		version := kv.KeyVersion[request.Key]
		value, ok := kv.KeyValue[request.Key]
		// kv.lastRequest[request.ClientId] = request.RequestId
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: value, Version: version, Err: rpc.OK}

	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// 检查重复请求
		if lastSeq, ok := kv.lastRequest[request.ClientId]; ok {
			if request.RequestId <= lastSeq {
				// [Debug] 发现重复
				// log.Printf("[DoOp] S%d DUP DETECTED Client=%d ID=%d. Return cached.",
				// 	kv.me, request.ClientId, request.RequestId)
				return kv.lastReply[request.ClientId]
			}
		}

		reply := rpc.PutReply{}
		// 检查键是否存在
		_, ok_value := kv.KeyValue[request.Key]
		// 在DoOp这一步可以记录操作了
		kv.lastRequest[request.ClientId] = request.RequestId
		if !ok_value {
			// 键不存在查看Put操作的version是不是0
			if request.Version == 0 {
				// 新插入一个pair
				kv.KeyVersion[request.Key] = 1
				kv.KeyValue[request.Key] = request.Value
				// return &rpc.PutReply{Err: rpc.OK}
				reply.Err = rpc.OK
			} else {
				// 不存在key也不是version = 0 那么就有问题返回
				// return &rpc.PutReply{Err: rpc.ErrNoKey}
				reply.Err = rpc.ErrNoKey
			}

		} else {
			// 键存在那么就更新
			version := kv.KeyVersion[request.Key]
			if version == request.Version {
				kv.KeyVersion[request.Key] = version + 1
				kv.KeyValue[request.Key] = request.Value
				// return &rpc.PutReply{Err: rpc.OK}
				reply.Err = rpc.OK
			} else {
				// return &rpc.PutReply{Err: rpc.ErrVersion}
				reply.Err = rpc.ErrVersion
			}
		}
		kv.lastRequest[request.ClientId] = request.RequestId
		kv.lastReply[request.ClientId] = reply

		return reply
	default:
		// 如果进到这里，说明 log 里有脏东西
		log.Printf("KVServer DoOp: unknown request type %T", req)
		return nil
	}
	// return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	// 把持久化数据序列化成[]byte发给rsm
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// db 内容
	e.Encode(kv.KeyVersion)
	e.Encode(kv.KeyValue)

	// 去重表
	e.Encode(kv.lastRequest)
	e.Encode(kv.lastReply)

	snap := w.Bytes()
	return snap
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	KeyVersion := make(map[string]rpc.Tversion)
	KeyValue := make(map[string]string)
	lastRequest := make(map[int64]int)
	lastReply := make(map[int64]any)

	if d.Decode(&KeyVersion) != nil ||
		d.Decode(&KeyValue) != nil ||
		d.Decode(&lastRequest) != nil ||
		d.Decode(&lastReply) != nil {
		log.Fatal("read persist kvsnap failed!")
	} else {
		kv.KeyVersion = KeyVersion
		kv.KeyValue = KeyValue

		kv.lastRequest = lastRequest
		kv.lastReply = lastReply
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	// 先在本地缓存client
	// 向rsm提交一个get log 得到确认之后再进行返回
	// log.Printf("server: %d recive client:%d Get() RequestId: %d,to submit", kv.me, args.ClientId, args.RequestId)
	rsmErr, res := kv.rsm.Submit(*args)
	// log.Printf("server: %d recive client:%d Get() RequestId: %d,submited, result is %s", kv.me, args.ClientId, args.RequestId,
	// 	rsmErr)
	// 检查系统层
	if rsmErr != rpc.OK {
		reply.Err = rsmErr
		return
	}

	// 检查业务返回
	if opReply, ok := res.(rpc.GetReply); ok {
		*reply = opReply
	} else {
		// 这里是系统断言失败和通信与业务均没关系
		reply.Err = rpc.ErrMaybe
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	// put与get 应该是一样的
	// log.Printf("server: %d recive client:%d PUT() RequestId: %d,to submit", kv.me, args.ClientId, args.RequestId)
	rsmErr, res := kv.rsm.Submit(*args)
	// log.Printf("server: %d recive client:%d Get() RequestId: %d,submited, result is %s", kv.me, args.ClientId, args.RequestId,
	// 	rsmErr)
	// 检查系统层
	if rsmErr != rpc.OK {
		reply.Err = rsmErr
		return
	}

	// 检查业务返回
	if opReply, ok := res.(rpc.PutReply); ok {
		*reply = opReply
	} else {
		// 这里是系统断言失败和通信与业务均没关系
		reply.Err = rpc.ErrMaybe
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})

	kv := &KVServer{me: me}
	kv.KeyValue = make(map[string]string)
	kv.KeyVersion = make(map[string]rpc.Tversion)
	kv.lastRequest = make(map[int64]int)
	kv.lastReply = make(map[int64]any)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
