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
	CurrentTerm uint64
	VotedFor    int
	Log         []LogEntry
	State       int
	Majority    int

	// volatile state on all servers
	CommitIndex     uint64
	LastApplied     uint64
	LastContect     time.Time
	ElectionTimeout time.Duration

	// volatile state on leaders
	// reinitialized after election
	NextIndex  []uint64
	MatchIndex []uint64

	// report applychannel
	// 由make()方法传入的channel
	// 向外部发送已提交的日志
	applych chan raftapi.ApplyMsg

	// 用于唤醒applier协程，当有新的log commit之后用它来发信号
	applyCond *sync.Cond
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

	// 涉及这三个变量的地方都要presist
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// encode the persisted state
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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

	var currentTerm uint64
	var votedFor int
	var mLog []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&mLog) != nil {
		log.Fatal("read persist failed!")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = mLog
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        uint64 // candidate's term
	VoteGrabted bool   //
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

	// 检查log的up-to-date
	// log长度和任期都要至少和me 一样大

	logIndex := uint64(len(rf.Log) - 1)
	logTerm := uint64(0)
	// log.Printf("server: %d checke the log", rf.me)
	// [1...n]
	if logIndex > 0 {
		logTerm = rf.Log[logIndex].Term
	}

	// 检查任期
	if args.LastLogTerm < logTerm {
		reply.VoteGrabted = false
		return
	}

	// 当任期一样的时候检查日志长度
	if args.LastLogTerm == logTerm {
		if args.LastLogIndex < logIndex {
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
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    uint64
	Success bool

	XTerm  uint64 //冲突条目term
	XIndex uint64 // 冲突任期号第一个索引
	XLen   uint64 // follower日志长度
}

type LogEntry struct {
	Command interface{} //raft只是安全管理物流包裹给KV服务器或是什么所以command是什么并不关心
	Term    uint64
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
	// log.Printf("Server %d timer reset because of heartbeat", rf.me)
	// 一致性检查

	lastindex := len(rf.Log) - 1

	// 一致性检查
	if args.PrevLogIndex > uint64(lastindex) {
		// 【关键日志】打印出拒绝的原因
		// log.Printf("[Follower S%d] Consistency check FAILED: PrevLogIndex %d is out of bounds (my log len is %d)",
		// 	rf.me, args.PrevLogIndex, len(rf.Log))
		reply.Success = false
		reply.XLen = uint64(len(rf.Log))
		reply.XTerm = 0
		reply.XIndex = 0
		return
	}

	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false

		reply.XTerm = rf.Log[args.PrevLogIndex].Term
		firstIndex := args.PrevLogIndex
		for firstIndex > 0 && rf.Log[firstIndex-1].Term == reply.XTerm {
			firstIndex--
		}
		reply.XIndex = firstIndex
		return
	}

	for i, newEntry := range args.Entries {

		if newEntry.Index >= len(rf.Log) {
			// 本地日志太短
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
		}
		// 一致的直接跳过
		if rf.Log[newEntry.Index].Term != newEntry.Term {
			// 发现了冲突点
			// rf.Log = rf.Log[:args.PrevLogIndex+1] // s[:n]->s[0]...s[n-1]
			// 截断至冲突点然后把后面的newEntry直接追加
			rf.Log = rf.Log[:newEntry.Index]
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
		}
	}
	// 追加完日志和修改过term之后就持久化

	// log.Printf("[Follower S%d] Consistency check PASSED.", rf.me)
	reply.Success = true

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, uint64(len(rf.Log)-1))

		// 唤醒applier
		// 当 commitIndex 前进时，唤醒自己的 applier 协程！
		rf.applyCond.Signal()
	}
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
	index = len(rf.Log)
	// 是leader的话打包收的命令
	newLogEntry := LogEntry{
		Command: command,
		Term:    rf.CurrentTerm,
		Index:   index,
	}

	// 添加到当前日志中
	rf.Log = append(rf.Log, newLogEntry)

	// local 日志变动持久化
	rf.persist()

	// 获取当前leader的日志索引
	// 1-indexed
	index = len(rf.Log) - 1
	term = int(rf.CurrentTerm)

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
		for rf.CommitIndex <= rf.LastApplied {
			// 如果没有新的已提交日志，就解锁并等待
			// wait()会原子的解锁rf.mu并让协程睡眠
			rf.applyCond.Wait()
			// 被唤醒时，wait 重新锁定rf.mu
		}

		// 记录在解锁前需要应用的日志范围
		lastApplied := rf.LastApplied
		commitIndex := rf.CommitIndex

		appliedEntries := make([]LogEntry, commitIndex-lastApplied)
		copy(appliedEntries, rf.Log[lastApplied+1:commitIndex+1])

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
		rf.LastApplied = commitIndex
		rf.mu.Unlock()
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

			if rf.NextIndex[serverIndex] < 1 {
				// 如果 nextIndex 已经回退到 0 或更小，说明已经退无可退
				// 此时不能再计算 previndex，否则会 panic
				// 最安全的做法是暂时放弃本次尝试，等待下一个心跳周期
				rf.mu.Unlock()
				return
			}
			previndex := rf.NextIndex[serverIndex] - 1
			prevterm := rf.Log[previndex].Term

			entriesToSend := rf.Log[rf.NextIndex[serverIndex]:]
			// 2. 创建一个全新的切片来存放副本
			entriesCopy := make([]LogEntry, len(entriesToSend))
			// 3. 将内容从原始日志复制到新切片中
			copy(entriesCopy, entriesToSend)

			reply := AppendEntriesReply{}
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: previndex,
				PrevLogTerm:  prevterm,
				Entries:      entriesCopy,
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
				newMatchIndex := args.PrevLogIndex + uint64(len(args.Entries))
				newNextIndex := newMatchIndex + 1

				// 【锦上添花的修复】
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
				// if args.PrevLogIndex == rf.NextIndex[serverIndex]-1 {
				// 	if rf.NextIndex[serverIndex] > 0 {
				// 		rf.NextIndex[serverIndex]--
				// 	}
				// }
				// 【【【 关键修复：实现快速回退！！！】】】
				if reply.XTerm == 0 { // 对应 Follower 日志太短的情况
					rf.NextIndex[serverIndex] = reply.XLen
				} else {
					// 尝试在 Leader 的日志中找到 XTerm
					found := false
					for i := len(rf.Log) - 1; i >= 0; i-- {
						if rf.Log[i].Term == reply.XTerm {
							// 找到了！直接跳到这个任期的下一个位置
							rf.NextIndex[serverIndex] = uint64(i + 1)
							found = true
							break
						}
					}
					if !found {
						// Leader 的日志里根本没有这个冲突任期，直接跳到冲突任期的第一个索引
						rf.NextIndex[serverIndex] = reply.XIndex
					}
				}
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
			// }
		}(i)
	}
}

// 收到entry回复之后更新commitindex
func (rf *Raft) updateCommitIndex() {
	// 从commitIndex 开始检查log
	for N := rf.CommitIndex + 1; N < uint64(len(rf.Log)); N++ {

		// leader只能commit自己当前任期的日志
		if rf.Log[N].Term != rf.CurrentTerm {
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
		} else {
			break
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
			lastindex := len(rf.Log) - 1
			lastterm := rf.Log[lastindex].Term
			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.me,
				LastLogIndex: uint64(lastindex),
				LastLogTerm:  lastterm,
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
	for i := range rf.peers {
		rf.NextIndex[i] = uint64(len(rf.Log))
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
	rf.NextIndex = make([]uint64, numServer)
	rf.MatchIndex = make([]uint64, numServer)

	// 为每个服务器初始化随机的timeoutduration
	rf.resetTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 传入channel
	rf.applych = applyCh
	// 初始化条件变量
	rf.applyCond = sync.NewCond(&rf.mu)

	// log.Printf("server %d initiated start to server!", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()

	// 启动applier协程
	go rf.Applier()

	return rf
}
