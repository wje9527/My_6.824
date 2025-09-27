package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	// 应该有一个map 记录key 和 version
	KeyVersion map[string]rpc.Tversion
	// 应该有一个map 记录key 和 value
	KeyValue map[string]string
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	// 初始化两个存储数据结构
	kv.KeyValue = make(map[string]string)
	kv.KeyVersion = make(map[string]rpc.Tversion)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 查询键
	value, ok := kv.KeyValue[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	version, ok := kv.KeyVersion[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = value
	reply.Version = version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 先检查键是否存在
	_, ok_value := kv.KeyValue[args.Key]
	if !ok_value {
		if args.Version == 0 {
			// put键不存在并且版本为0可以插入
			kv.KeyValue[args.Key] = args.Value
			kv.KeyVersion[args.Key] = 1
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	} else {
		// 存在的键要检查version，version最新则进行更新，否则返回err
		version, ok_version := kv.KeyVersion[args.Key]

		if !ok_version {
			reply.Err = rpc.ErrNoKey
			return
		}

		if version == args.Version {
			kv.KeyValue[args.Key] = args.Value
			kv.KeyVersion[args.Key] = version + 1
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
		return
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
