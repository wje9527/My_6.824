package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId  int
	clientId  int64 //随机生成
	requestId int   // 自增请求ID
	// mu        sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.

	args := rpc.GetArgs{Key: key, ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId++
	reply := rpc.GetReply{}
	// currentLeader = ck.leader
	// 首先尝试缓存的leader，缓存leader失效之后再去其他server查询
	serverId := ck.leaderId
	for {
		//每次循环清空
		reply = rpc.GetReply{}
		// log.Printf("client:%d call get() to server:%d,with requestId:%d", args.ClientId, serverId, args.RequestId)
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrTimeOut || reply.Err == rpc.ErrWrongLeader {
			// 这几个错误进行重试
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}

		ck.leaderId = serverId
		return reply.Value, reply.Version, reply.Err
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId++
	reply := rpc.PutReply{}
	serverId := ck.leaderId
	for {
		reply = rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
		if ok {
			// 得到了一个从服务器返回的回复
			// 判断返回类型
			if reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrTimeOut {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
			// 其他回复则直接返回给客户端
			ck.leaderId = serverId
			return reply.Err
		}
		// 同样切换服务器
		serverId = (serverId + 1) % len(ck.servers)
	}
}
