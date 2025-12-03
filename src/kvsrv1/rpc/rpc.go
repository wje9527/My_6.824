package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"
	ErrTimeOut = "ErrTimeOut"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
	// 用来对于重复请求去重
	ClientId  int64
	RequestId int
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// 用来标记请求和发送者
	ClientId  int64
	RequestId int
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}
