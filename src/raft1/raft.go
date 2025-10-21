package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	Leader = iota
	Candidate
	Follower
)

const (
	// 选举超时间隔 300 - （300+200） 500 容忍时间
	ElectionTimeoutLowerBound = 300
	ElectionTimeoutRange      = 200

	// 心跳间隔
	HeartBeatInterval = 50 * time.Millisecond
)

const (
	NotVoted = -1
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state on all servers
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	State       int
	Majority    int

	// volatile state on all servers
	CommitIndex     int
	LastApplied     int
	LastContect     time.Time
	ElectionTimeout time.Duration

	// volatile state on leaders
	// reinitialized after election
	NextIndex  []int
	MatchIndex []int

	// report applychannel
	// 由make()方法传入的channel
	// 向外部发送已提交的日志
	applych chan raftapi.ApplyMsg

	// 用于唤醒applier协程，当有新的log commit之后用它来发信号
	applyCond *sync.Cond

	// 3D加入snapshot
	LastIncludedIndex int
	LastIncludedTerm  int

	// 管理并发
	// inFlight []bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = int(rf.CurrentTerm)
	if rf.State == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// convert the logicIndex to physicIndex
func (rf *Raft) ConvertToPhysicIdx(logicIndex int) (int, bool) {
	// 物理索引 = 逻辑索引 - lastincludedIndex
	// 调用时并发情况下要锁
	// 逻辑索引小于snapshot包含的索引也就是基址，无效

	// 加入snapshot之后所有raft结构里面存储的索引都是逻辑索引了
	if logicIndex < rf.LastIncludedIndex {
		return 0, false //无效索引
	}

	physicIndex := logicIndex - rf.LastIncludedIndex

	// 检查physicIndex的合法性
	if physicIndex >= len(rf.Log) {
		return 0, false
	}

	return physicIndex, true
}

// 获取最后一个logentry的index和term
// 无锁版本
func (rf *Raft) GetLastLog() LogEntry {

	lastEntry := rf.Log[len(rf.Log)-1]

	return lastEntry
}

// 转化物理索引到逻辑索引
func (rf *Raft) ConvertToLogicIdx(physicIndex int) (int, bool) {
	// 检查物理索引的合法性
	if physicIndex < 0 || physicIndex >= len(rf.Log) {
		return 0, false
	}

	logicIndex := rf.LastIncludedIndex + physicIndex

	return logicIndex, true
}

// 通过逻辑所以返回一个对应的日志项
func (rf *Raft) GetLogEntry(logicalindex int) (LogEntry, bool) {

	// 逻辑索引小于下界
	if logicalindex < rf.LastIncludedIndex {
		return LogEntry{}, false
	}

	localIndex := logicalindex - rf.LastIncludedIndex

	// 物理索引不超过上界
	if localIndex >= len(rf.Log) {
		return LogEntry{}, false
	}

	return rf.Log[localIndex], true
}

// 通过逻辑索引返回一段日志切片
func (rf *Raft) GetLogSlices(logicalstart int, logicalend int) ([]LogEntry, bool) {

	localstart := logicalstart - rf.LastIncludedIndex
	localend := logicalend - rf.LastIncludedIndex

	// log.Printf("[server %d][logicalstart is %d , logicalend is %d], [localstart is %d, localend is %d], [lastincludeindex is %d]", rf.me, logicalstart,
	// 	logicalend, localstart, localend, rf.LastIncludedIndex)
	if localstart < 0 || localend > len(rf.Log) {
		return nil, false
	}

	logslice := rf.Log[localstart:localend]
	newslices := make([]LogEntry, len(logslice))
	// 遵循raft默认返回[s:e] = [s...e-1]
	copy(newslices, logslice)

	return newslices, true
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	raftstate := rf.EncodeRaftState()
	mSnapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, mSnapshot)
}

func (rf *Raft) EncodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)

	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var mLog []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&mLog) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		log.Fatal("read persist failed!")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = mLog
		rf.LastIncludedIndex = lastIncludeIndex
		rf.LastIncludedTerm = lastIncludeTerm
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// struct for RPC args
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// Offset            int   not implemented
	Data []byte
	// Done bool		not implemented
}

// struct for PRC replys
type InstallSnapshotReply struct {
	Term int
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Printf("[S%d T%d SNAPSHOT] Index: %d, Term: %d. New log len: %d, New LastIncludedIndex: %d",
	// 	rf.me, rf.CurrentTerm, index, rf.LastIncludedTerm, len(rf.Log), rf.LastIncludedIndex)

	// 切片已经过期，包含过了
	if index <= rf.LastIncludedIndex {
		return
	}

	// 检查index是否在快照中
	entryAtIndex, ok := rf.GetLogEntry(index)

	if !ok {
		// index 不存在
		return
	}

	// 切片删除原有log
	// lastIncludedTerm := entryAtIndex.Term
	// logStartIndex := index - rf.LastIncludedIndex + 1

	// logTail := rf.Log[logStartIndex:]

	// newLog := make([]LogEntry, 1+len(logTail))

	// // 加入占位符
	// newLog[0] = entryAtIndex

	// copy(newLog[1:], logTail)
	// 替换原日志
	logStartIndex := index - rf.LastIncludedIndex
	rf.Log = rf.Log[logStartIndex:]

	// 更新rf数据
	rf.LastIncludedIndex = index
	// rf.LastIncludedTerm = lastIncludedTerm
	rf.LastIncludedTerm = entryAtIndex.Term

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// encode the persisted state
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)

	// 可以在这里打印日志确认截断后的状态
	// log.Printf("[S%d T%d SNAPSHOT] Done. New log len: %d, New LastIncludedIndex: %d, New LastIncludedTerm: %d",
	// 	rf.me, rf.CurrentTerm, len(rf.Log), rf.LastIncludedIndex, rf.LastIncludedTerm)
}

// the installSnapshot RPC
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 加上reply的任期
	reply.Term = rf.CurrentTerm
	// 检查snapshot的任期
	if args.Term < rf.CurrentTerm {
		// rf.mu.Unlock()
		return
	}

	// leader任期更新也要更新
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = NotVoted
	}

	// 改变自己的状态 并且重置计时器
	rf.State = Follower
	rf.resetTimer()

	// 过时快照直接丢弃
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		// rf.mu.Unlock()
		return
	}

	// 检查snapshot 的index判断是重新装载log还是对原有的进行截断
	// data 是提交给上层应用的数据

	// 所有日志都已经过期直接全部丢掉
	newLog := make([]LogEntry, 1)
	newLog[0].Index = args.LastIncludedIndex
	newLog[0].Term = args.LastIncludedTerm

	// lastlog := rf.GetLastLog()
	// if lastlog.Index >= args.LastIncludedIndex {
	// 	matchEntry, ok := rf.GetLogEntry(args.LastIncludedIndex)

	// 	if ok && matchEntry.Term == args.LastIncludedTerm {
	// 		// 成功找到匹配的日志截断项
	// 		// 把尾部加入newlog
	// 		logTail, _ := rf.GetLogSlices(matchEntry.Index+1, lastlog.Index+1)

	// 		newLog = append(newLog, logTail...)
	// 	}
	// }

	// 修改log
	rf.Log = newLog

	// 修改元数据
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm

	// 修改raftstate
	raftstate := rf.EncodeRaftState()
	rf.persister.Save(raftstate, args.Data)

	// // 唤醒applier
	rf.applyCond.Signal()

	// rf.mu.Unlock()
}

// 发送snapshot
func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.State != Leader {
		return
	}
	rf.mu.Unlock()
	// 发起rpc
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 检查前后任期,回复的有效性
	if args.Term != rf.CurrentTerm {
		return
	}

	// 检查回复的任期大小
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.State = Follower
		rf.persist()
		return
	}

	// 同步更新follower的match 和next idx
	if args.LastIncludedIndex > rf.MatchIndex[server] {
		rf.MatchIndex[server] = args.LastIncludedIndex
	}

	if args.LastIncludedIndex+1 > rf.NextIndex[server] {
		rf.NextIndex[server] = args.LastIncludedIndex + 1
	}

	rf.updateCommitIndex()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // candidate's term
	VoteGrabted bool //
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// 由候选者调用来收集投票
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 过期投票直接返回false 并且让candidate 更新自己的任期
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGrabted = false
		return
	}

	if args.Term > rf.CurrentTerm {
		// 自己的term已经过期了，那么就要清空投票状态并且修改任期
		rf.CurrentTerm = args.Term
		rf.VotedFor = NotVoted
		rf.State = Follower
		// 投票完成后持久化
		rf.persist()
	}

	reply.Term = rf.CurrentTerm
	// 当前任期已经投过票 投了票且投的不是请求方
	if (rf.VotedFor != NotVoted) && (rf.VotedFor != args.CandidateID) {
		reply.VoteGrabted = false
		return
	}

	// 改成逻辑索引的比较
	lastLog := rf.GetLastLog()

	// 检查任期
	if args.LastLogTerm < lastLog.Term {
		reply.VoteGrabted = false
		return
	}

	// 当任期一样的时候检查日志长度
	if args.LastLogTerm == lastLog.Term {
		if args.LastLogIndex < lastLog.Index {
			reply.VoteGrabted = false
			return
		}
	}

	//通过检查可以投票
	// log.Printf("server: %d vote to server: %d, at term %d", rf.me, args.CandidateID, args.Term)
	reply.VoteGrabted = true
	rf.VotedFor = args.CandidateID

	// 投票完成后持久化
	rf.persist()
	// 投票后要重置自己的选举计时器
	rf.resetTimer()
	// log.Printf("Server %d timer reset because of voted", rf.me)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC args structure
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int //冲突条目term
	XIndex int // 冲突任期号第一个索引
	XLen   int // follower日志长度
}

type LogEntry struct {
	Command interface{} //raft只是安全管理物流包裹给KV服务器或是什么所以command是什么并不关心
	Term    int
	Index   int
}

// appendentries 接口用来接收从leader传来的心跳包或者
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// 判断term
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	// 状态转换
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = NotVoted
		rf.State = Follower
	}

	// 合法心跳要投降
	reply.Term = rf.CurrentTerm
	rf.State = Follower

	// 收到心跳重置计时器
	rf.resetTimer()

	// 日志一致检查
	if !rf.checkLogConsistency(args, reply) {
		return
	}

	lastlog := rf.GetLastLog()
	for i, newEntry := range args.Entries {

		if newEntry.Index > lastlog.Index {
			// 本地日志太短
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
		}
		// 一致的直接跳过

		curEntry, _ := rf.GetLogEntry(newEntry.Index)
		if curEntry.Term != newEntry.Term {
			// 发现了冲突点
			// 截断至冲突点然后把后面的newEntry直接追加
			arrEndIdx, _ := rf.ConvertToPhysicIdx(curEntry.Index)
			rf.Log = rf.Log[:arrEndIdx]
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
		}
	}
	// 追加完日志和修改过term之后就持久化

	// log.Printf("[Follower S%d] Consistency check PASSED.", rf.me)
	reply.Success = true

	lastlog = rf.GetLastLog()
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, lastlog.Index)

		// 唤醒applier
		// 当 commitIndex 前进时，唤醒自己的 applier 协程！
		rf.applyCond.Signal()
	}
}

// 检查appendentries 的日志一致性
func (rf *Raft) checkLogConsistency(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// 检查日志是否过期
	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		// reply.XIndex = 0
		// reply.XLen = 0
		reply.XLen = rf.LastIncludedIndex + 1
		return false
	}

	prevLog, ok := rf.GetLogEntry(args.PrevLogIndex)
	lastLog := rf.GetLastLog()

	// 本地日志太短了，前面已经检查过
	if !ok {
		reply.Success = false
		reply.XIndex = 0
		reply.XTerm = 0
		reply.XLen = lastLog.Index + 1
		return false
	}

	// 检查任期冲突
	if prevLog.Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = prevLog.Term
		firstIndex := prevLog.Index
		// 找到冲突任期的第一个索引
		for firstIndex > rf.LastIncludedIndex {
			prevEntry, ok := rf.GetLogEntry(firstIndex - 1) //看前一个entry

			if !ok || prevEntry.Term != reply.XTerm {
				break
			}

			firstIndex--
		}

		reply.XIndex = firstIndex
		return false
	}

	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) resetTimer() {
	rf.LastContect = time.Now()

	ms := ElectionTimeoutLowerBound + (rand.Int63() % ElectionTimeoutRange)
	rf.ElectionTimeout = time.Duration(ms) * time.Millisecond
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// RAFT受到客户端指令然后追加到自己的日志中并向follower发送
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	rf.mu.Lock()

	// 检查是否是leader，只有leader才能进行操作
	if rf.State != Leader {
		isLeader = false
		// log.Printf("server %d 不是leader 退出测试", rf.me)
		rf.mu.Unlock()
		return index, term, isLeader
	}

	// log.Printf("server %d 是leader 正常测试", rf.me)
	lastlog := rf.GetLastLog()
	newLogIndex := lastlog.Index + 1
	// 是leader的话打包收的命令
	newLogEntry := LogEntry{
		Command: command,
		Term:    rf.CurrentTerm,
		Index:   newLogIndex,
	}

	// 添加到当前日志中
	rf.Log = append(rf.Log, newLogEntry)

	// local 日志变动持久化
	rf.persist()

	// 获取当前leader的日志索引
	// 1-indexed
	index = newLogEntry.Index
	term = rf.CurrentTerm

	lastlog = rf.GetLastLog()
	// log.Printf("[START S%d T%d] New entry added. New LastLogIndex: %d, Term: %d",
	// 	rf.me, rf.CurrentTerm, lastlog.Index, lastlog.Term)

	rf.mu.Unlock()

	rf.BroadcastEntries()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// kill applier
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker是一个raft服务器的唯一时钟
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.State
		rf.mu.Unlock()

		// log.Printf("[Server %d Term %d State %v ] wakeup", rf.me, rf.CurrentTerm, rf.State)
		if state == Leader {
			// leader 要定期向其它服务器发送heart beat
			// log.Printf("[Server %d Term %d State %v ] send heartbeat", rf.me, rf.CurrentTerm, rf.State)
			rf.BroadcastEntries()
			time.Sleep(HeartBeatInterval)
		} else {
			// 如果是follower那要检查上次收到心跳是什么时候有没有超过间隔
			rf.mu.Lock()
			isTimeout := time.Since(rf.LastContect) > rf.ElectionTimeout
			rf.mu.Unlock()

			if isTimeout {
				// log.Printf("[Server %d Term %d] Election timer expired! State: %v -> Candidate", rf.me, rf.CurrentTerm, rf.State)
				// 超时要发起选举
				rf.AttemptElection()
			}
			time.Sleep(10 * time.Millisecond)
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) Applier() {
	// 一直循环到服务器kill
	for !rf.killed() {
		rf.mu.Lock()

		// 条件变量
		for rf.CommitIndex <= rf.LastApplied && rf.LastApplied >= rf.LastIncludedIndex {
			// 如果没有新的已提交日志，就解锁并等待
			// wait()会原子的解锁rf.mu并让协程睡眠
			rf.applyCond.Wait()
			// 被唤醒时，wait 重新锁定rf.mu
		}
		// DPrintf("[Applier] server %d calling the allpier LastIncludeTerm:%d, LastIncludeIndex:%d", rf.me, rf.LastIncludedTerm, rf.LastIncludedIndex)
		// 加入检查是否有快照需要应用
		if rf.LastApplied < rf.LastIncludedIndex {
			// log.Printf("[Applier] server %d applying.LastIncludeTerm:%d, LastIncludeIndex:%d", rf.me, rf.LastIncludedTerm, rf.LastIncludedIndex)
			applyMsg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.LastIncludedTerm,
				SnapshotIndex: rf.LastIncludedIndex,
			}

			lastIncludedIndex := rf.LastIncludedIndex

			rf.mu.Unlock()
			rf.applych <- applyMsg

			rf.mu.Lock()
			// if rf.LastApplied < lastIncludedIndex {
			// 	rf.LastApplied = lastIncludedIndex
			// }
			rf.LastApplied = max(rf.LastApplied, lastIncludedIndex)
			// log.Printf("[Applier] server %d applied Done LastApplied:%d", rf.me, rf.LastApplied)
			rf.mu.Unlock()
			continue
		} else {
			// 记录在解锁前需要应用的日志范围
			lastApplied := rf.LastApplied
			commitIndex := rf.CommitIndex

			// appliedEntries := make([]LogEntry, commitIndex-lastApplied)
			// copy(appliedEntries, rf.Log[lastApplied+1:commitIndex+1])
			appliedEntries, ok := rf.GetLogSlices(lastApplied+1, commitIndex+1)

			if !ok || len(appliedEntries) == 0 {
				rf.mu.Unlock()
				continue
			}

			// 解锁之后让其他协程进行
			rf.mu.Unlock()

			// 把entries 发送到管道
			for _, entry := range appliedEntries {
				rf.applych <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
			}

			rf.mu.Lock()
			rf.LastApplied = max(rf.LastApplied, commitIndex)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) BroadcastEntries() {
	rf.mu.Lock()

	// 检查领导权
	if rf.State != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	// 给每个服务器发送心跳
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// heartbeat entries 为空

		go func(serverIndex int) {
			// for !rf.killed() {
			rf.mu.Lock()
			// 检查领导权
			if rf.State != Leader || rf.killed() {
				rf.mu.Unlock()
				return
			}

			// 从逻辑转到物理索引
			nextIndex := rf.NextIndex[serverIndex]
			lastLog := rf.GetLastLog()
			if nextIndex <= rf.LastIncludedIndex {
				// log.Printf("[Sendsnap S%d T%d] -> S%d | Preparing to send. nextIndex: %d, current lastLogIndex: %d",
				// 	rf.me, rf.CurrentTerm, serverIndex, nextIndex, lastLog.Index)
				args := InstallSnapshotArgs{
					Term:              rf.CurrentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.LastIncludedIndex,
					LastIncludedTerm:  rf.LastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				reply := InstallSnapshotReply{}
				// 如果follower的next 小于leader的当前log,那么直接发送snapshot
				rf.mu.Unlock()
				rf.sendSnapShot(serverIndex, &args, &reply)
				return
			}

			// log.Printf("[heartbeat S%d T%d] -> S%d | Preparing to send. nextIndex: %d, current lastLogIndex: %d",
			// 	rf.me, rf.CurrentTerm, serverIndex, nextIndex, lastLog.Index)
			previndex := nextIndex - 1
			prevLog, _ := rf.GetLogEntry(previndex)
			// lastLog := rf.GetLastLog()

			entriesToSend, slice_ok := rf.GetLogSlices(nextIndex, lastLog.Index+1)
			if !slice_ok {
				rf.mu.Unlock()
				return
			}

			reply := AppendEntriesReply{}
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: previndex,
				PrevLogTerm:  prevLog.Term,
				Entries:      entriesToSend,
				LeaderCommit: rf.CommitIndex,
			}
			rf.mu.Unlock()
			// log.Printf("[S%d T%d ->S%d PrevIdx=%d PrevTerm=%d lenE=%d nextIndex=%d commit=%d]",
			// 	rf.me, rf.CurrentTerm, serverIndex, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), rf.NextIndex[serverIndex], rf.CommitIndex)

			ok := rf.sendAppendEntries(serverIndex, &args, &reply)

			// 网络失败直接返回
			if !ok {
				return
			}

			// 成功之后可能需要修改状态
			rf.mu.Lock()
			// log.Printf("[Leader S%d T%d] <- S%d, Got AppendEntries Reply. Success: %v, ReplyTerm: %d",
			// rf.me, rf.CurrentTerm, serverIndex, reply.Success, reply.Term)
			// 检查回复有没有超期,超期了直接返回
			if args.Term != rf.CurrentTerm {
				rf.mu.Unlock()
				return
			}

			// 没有超期则进行检查
			// 自己的任期过期
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.State = Follower
				rf.VotedFor = NotVoted
				rf.persist()
				rf.mu.Unlock()
				return
			}

			// 任期没过期，同步log同步点
			if reply.Success {
				// 计算这次成功回复对应的新 matchIndex 和 nextIndex
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				newNextIndex := newMatchIndex + 1
				// 只在新的认知比旧的认知更“进步”时才更新
				if newNextIndex > rf.NextIndex[serverIndex] {
					rf.NextIndex[serverIndex] = newNextIndex
				}
				if newMatchIndex > rf.MatchIndex[serverIndex] {
					rf.MatchIndex[serverIndex] = newMatchIndex
				}

				// 收到成功回复就检查是否达到大多数
				rf.updateCommitIndex()
				rf.mu.Unlock()
				return
			} else {
				// log.Printf("[Leader S%d T%d -> S%d 失败回复] 处理拒绝。",
				// 	rf.me, rf.CurrentTerm, serverIndex)
				// 实现快速回退
				if reply.XTerm == 0 { // 对应 Follower 日志太短的情况
					// log.Printf("[Leader S%d T%d -> S%d 快速回退] Follower 日志太短。设置 nextIndex 为 XLen: %d",
					// 	rf.me, rf.CurrentTerm, serverIndex, reply.XLen)
					rf.NextIndex[serverIndex] = reply.XLen
				} else {
					// 尝试在 Leader 的日志中找到 XTerm
					found := false
					for i := len(rf.Log) - 1; i >= 0; i-- {
						if rf.Log[i].Term == reply.XTerm {
							// 找到了！直接跳到这个任期的下一个位置
							// log.Printf("[Leader S%d T%d -> S%d 快速回退] 找到任期 %d 的最后条目在逻辑索引 %d。",
							// 	rf.me, rf.CurrentTerm, serverIndex, reply.XTerm, rf.Log[i].Term)
							rf.NextIndex[serverIndex] = rf.Log[i].Index + 1
							found = true
							break
						}
					}
					if !found {
						// Leader 的日志里根本没有这个冲突任期，直接跳到冲突任期的第一个索引
						rf.NextIndex[serverIndex] = reply.XIndex
					}
					// log.Printf("[heartbeat S%d T%d] -> S%d | Preparing to send. nextIndex: %d, current lastLogIndex: %d",
					// 	rf.me, rf.CurrentTerm, serverIndex, nextIndex, lastLog.Index)
				}
				rf.mu.Unlock()

				// Follower 有冲突任期 XTerm，其第一个索引是 XIndex

				// 尝试在 Leader 的日志中查找任期为 XTerm 的【最后一条】日志
				// lastLog := rf.GetLastLog()
				// leaderLastIndexWithXTerm := -1 // 初始化为未找到

				// 从 Leader 当前日志的末尾向前查找 (使用逻辑索引)
				// 查找范围应该从 PrevLogIndex (导致失败的位置) 向前，直到快照点之后
				// 但为了简单和覆盖所有情况，可以直接从 Leader 的最后日志向前找
				// for logicalIndex := lastLog.Index; logicalIndex > rf.LastIncludedIndex; logicalIndex-- {
				// 	entry, ok := rf.GetLogEntry(logicalIndex)
				// 	if ok && entry.Term == reply.XTerm {
				// 		// 找到了 Leader 日志中 XTerm 的最后一条日志
				// 		leaderLastIndexWithXTerm = logicalIndex
				// 		break
				// 	}
				// 	// 如果 entry.Term < reply.XTerm，说明再往前找也不会有 XTerm 了，可以提前退出优化
				// 	if ok && entry.Term < reply.XTerm {
				// 		break
				// 	}
				// }

				// if leaderLastIndexWithXTerm != -1 {
				// 	// Leader 在自己的日志中找到了 XTerm，
				// 	// 将 nextIndex 设置为 Leader 中该 Term 最后一条日志的【下一条】
				// 	rf.NextIndex[serverIndex] = leaderLastIndexWithXTerm + 1
				// } else {
				// 	// Leader 的日志中没有 XTerm (或者 XTerm 只存在于快照中)
				// 	// 将 nextIndex 直接设置为 Follower 冲突任期的【第一个】索引
				// 	rf.NextIndex[serverIndex] = reply.XIndex
				// }
				// // 【健壮性检查】: 确保 nextIndex 至少为 1 (或 LastIncludedIndex + 1)
				// if rf.NextIndex[serverIndex] <= rf.LastIncludedIndex {
				// 	rf.NextIndex[serverIndex] = rf.LastIncludedIndex + 1
				// }
				time.Sleep(10 * time.Millisecond)
			}
			// }
		}(i)
	}
}

// 收到entry回复之后更新commitindex
func (rf *Raft) updateCommitIndex() {

	lastlog := rf.GetLastLog()
	// 从commitIndex 开始检查log
	for N := rf.CommitIndex + 1; N < lastlog.Index+1; N++ {

		// leader只能commit自己当前任期的日志
		// if rf.Log[N].Term != rf.CurrentTerm {
		// 	continue
		// }
		curEntry, _ := rf.GetLogEntry(N)
		if curEntry.Term != rf.CurrentTerm {
			continue
		}

		// 统计有多少个节点复制了索引为N的日志
		count := 1

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			if rf.MatchIndex[i] >= N {
				count++
			}
		}

		if count >= rf.Majority {
			// 更新commitindexid
			rf.CommitIndex = N
			// log.Printf("[Leader S%d T%d] CommitIndex advanced to %d", rf.me, rf.CurrentTerm, rf.CommitIndex)
			// 唤醒applier
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	// 转变自己为candidate
	rf.State = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	voteSum := 1
	finished := 1
	// log.Printf("server %d attempt to election at term %d", rf.me, rf.CurrentTerm)
	// 启动选举之后重置自己的选举计时器
	rf.resetTimer()
	// 发起选举后持久化
	rf.persist()
	rf.mu.Unlock()
	condi := sync.NewCond(&rf.mu)
	// 向大家发送投票请求
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(serverIndex int) {
			rf.mu.Lock()
			lastlog := rf.GetLastLog()
			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastlog.Index,
				LastLogTerm:  lastlog.Term,
			}
			var reply RequestVoteReply
			rf.mu.Unlock()
			ok := rf.sendRequestVote(serverIndex, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 网络问题直接返回
			if !ok {
				finished++
				condi.Broadcast()
				return
			}

			// 确保自己仍是condidate并且任期没变
			if rf.State != Candidate || rf.CurrentTerm != args.Term {
				finished++
				condi.Broadcast()
				return
			}

			// 如果发现自己任期过期那么立刻放弃并更新任期
			if reply.Term > rf.CurrentTerm {
				rf.State = Follower
				rf.CurrentTerm = reply.Term
				finished++
				rf.persist()
				condi.Broadcast()
				return
			}

			// 统计票数
			if reply.VoteGrabted {
				voteSum++
			}
			finished++
			condi.Broadcast()
		}(i)
	}

	rf.mu.Lock()
	for voteSum < rf.Majority && finished < len(rf.peers) {
		condi.Wait()
	}

	if voteSum >= rf.Majority {
		// log.Printf("server %d become leader at term %d", rf.me, rf.CurrentTerm)
		rf.BecomeLeader()
		rf.mu.Unlock()
		rf.BroadcastEntries()
		return
	}
	rf.mu.Unlock()
}

func (rf *Raft) BecomeLeader() {
	rf.State = Leader
	lastlog := rf.GetLastLog()
	for i := range rf.peers {
		rf.NextIndex[i] = lastlog.Index + 1
		rf.MatchIndex[i] = 0
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{} // 初始化list
	// 初始log为空所以长度为0
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	numServer := len(rf.peers)

	// persistent state
	rf.CurrentTerm = 0
	rf.VotedFor = NotVoted
	rf.Log = make([]LogEntry, 1) //占一符

	// volatile state
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.State = Follower
	rf.Majority = numServer/2 + 1

	// volatile state on leader
	rf.NextIndex = make([]int, numServer)
	rf.MatchIndex = make([]int, numServer)

	// 为每个服务器初始化随机的timeoutduration
	rf.resetTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 传入channel
	rf.applych = applyCh
	// 初始化条件变量
	rf.applyCond = sync.NewCond(&rf.mu)

	// 初始化并发控制变量
	// rf.inFlight = make([]bool, numServer)

	// log.Printf("server %d initiated start to server!", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()

	// 启动applier协程
	go rf.Applier()

	return rf
}
