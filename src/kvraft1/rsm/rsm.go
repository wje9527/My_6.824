package rsm

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  int64
	Me  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	notifyCh map[int64]chan OpResult
	// lastApplied int
	dead  int32
	seqid int64
}

type OpResult struct {
	// Err   rpc.Err
	Value any
	Term  int
	OpId  int64
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		notifyCh:     make(map[int64]chan OpResult),
		sm:           sm,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	// 检查是否有snapshot，有的话应该从snapshot里面加载
	snapshot := persister.ReadSnapshot()
	// 如果snapshot长度大于0那么需要从snapshot恢复
	if len(snapshot) > 0 {
		sm.Restore(snapshot)
	}

	go rsm.Reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	// 向raft发起一次日志同步

	// 新创建一个Op
	rsm.mu.Lock()
	if rsm.killed() { // 检查自己是否已死
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // 直接拒绝
	}

	newOp := Op{
		Id:  rsm.generateOpId(),
		Me:  rsm.me,
		Req: req,
	}

	// 先使用opId进行chan注册
	ch := make(chan OpResult, 1)
	rsm.notifyCh[newOp.Id] = ch
	defer func() {
		rsm.mu.Lock()
		delete(rsm.notifyCh, newOp.Id)
		rsm.mu.Unlock()
	}()

	rsm.mu.Unlock()
	// 传达给raft
	// logIdx, term, isLeader := rsm.rf.Start(newOp)
	_, term, isLeader := rsm.rf.Start(newOp)

	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	// log.Printf("S%d Submit Id=%d logIdx=%d term=%d", rsm.me, newOp.Id, logIdx, term)
	// rsm.mu.Lock()
	// ch := make(chan OpResult, 1)
	// rsm.notifyCh[logIdx] = ch
	// rsm.mu.Unlock()
	// rsm.mu.Lock()
	// ch := make(chan OpResult, 1)

	// rsm.notifyCh[logIdx] = ch
	// rsm.mu.Unlock()
	// defer func() {
	// 	rsm.mu.Lock()
	// 	delete(rsm.notifyCh, logIdx)
	// 	rsm.mu.Unlock()
	// }()
	// 映射logidx对应的opid
	// rsm.mu.Lock()
	// rsm.indexToOpId[logIdx] = newOp.Id
	// rsm.mu.Unlock()
	// log.Printf("Server %d, Submited Op %d, idx: %d, term: %d", newOp.Me, newOp.Id, logIdx, term)

	// 等待结果
	// 设置检测ticker
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	// 循环检测leader状态
	for {
		select {
		case result := <-ch:
			// 查看消息有没有过期
			if result.OpId == newOp.Id {
				return rpc.OK, result.Value
			}
			// log.Printf("Server %d, Commit Op %d, idx: %d, term: %d", newOp.Me, newOp.Id, logIdx, term)
			// 可以返回结果
			return rpc.ErrWrongLeader, nil
		case <-ticker.C:
			currentTerm, isLeader := rsm.rf.GetState()
			if !isLeader || currentTerm != term {
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Lock()
			if rsm.killed() {
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			// if logIdx < rsm.lastApplied {
			// 	// submit错过了apply的结果
			// 	rsm.mu.Unlock()
			// 	return rpc.ErrWrongLeader, nil
			// }
			rsm.mu.Unlock()

		case <-time.After(2 * time.Second):
			return rpc.ErrTimeOut, nil
		}
	}

}

// 可以使用snowflake方法
func (rsm *RSM) generateOpId() int64 {
	// 直接用纳秒时间戳。
	now := time.Now().UnixNano()

	seqId := atomic.AddInt64(&rsm.seqid, 1)

	id := (now << 20) | (seqId & 0xFFFFF)
	return id
}

func (rsm *RSM) Kill() {
	atomic.AddInt32(&rsm.dead, 1)
	rsm.rf.Kill()
}

func (rsm *RSM) killed() bool {
	r := atomic.LoadInt32(&rsm.dead)
	return r == 1
}

func (rsm *RSM) Reader() {
	// reader始终读取raft层applied的msg
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			// 获得了消息要包装给submit
			// 把command 拆解成原来的OP 类似于C++的dynamic_cast
			op, ok := msg.Command.(Op)

			if !ok {
				log.Println("msg.command 拆解 op 出错")
			}
			rsm.mu.Lock()

			// 记录snapshot的截断点
			val := rsm.sm.DoOp(op.Req)

			// 检查raft的状态大小 是否需要进行snapshot
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
				//需要进行,调用状态机的snapshot方法
				snapshot := rsm.sm.Snapshot()
				// 告诉raft
				rsm.rf.Snapshot(msg.CommandIndex, snapshot)
			}
			// 找到之前submit的chan 然后发送给他
			// if <初始化语句>; <判断条件>
			// if ch, match := rsm.notifyCh[msg.CommandIndex]; match {
			if ch, match := rsm.notifyCh[op.Id]; match {
				// 向ch 发送resultOp
				ch <- OpResult{
					Value: val,
					Term:  msg.CommandTerm,
					OpId:  op.Id,
				}
				//发送之后删除submit
				// delete(rsm.notifyCh, msg.CommandIndex)
				delete(rsm.notifyCh, op.Id)
			}
			// 解锁
			rsm.mu.Unlock()
		} else {
			// commandvalid 是 false说明是snapshot
			// rsm应该通过snapshot来恢复业务
			if msg.SnapshotValid {
				// 恢复
				rsm.sm.Restore(msg.Snapshot)
			}
		}
	}
	rsm.Kill()
}
