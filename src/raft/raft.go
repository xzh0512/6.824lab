package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"syscall"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Status int

const (
	Follower Status = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	rw        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//节点状态
	state Status
	//当前任期
	currentTerm int
	//投票权
	votedFor int
	//心跳时间
	heartTime int
	//预写入日志
	log []LogEntry
	//提交索引
	commitIndex int
	//要发送给下条日志的索引
	nextIndex []int
	//用于跟踪每个服务器上已成功复制的最高日志条目的索引
	matchIndex []int
	//日志都存储在这里(2B)
	applyChan chan ApplyMsg
	//已放入管道的日志
	applyIndex       int
	heartBeatTimeout time.Duration
	eventChan        chan struct{}
	heartChan        chan struct{}
	voteN            int
	//是否投票已经超过半数
	isVoteN bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.rw.RLock()
	isleader = rf.state == Leader
	term = rf.currentTerm
	rf.rw.RUnlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.log) != nil || e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil {
		panic("译码出错！！！")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	var log []LogEntry
	var currentTerm, votedFor int
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	if d.Decode(&log) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil {
		panic("解码出错！！！")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = make([]LogEntry, len(log))
		copy(rf.log, log)
		//PrettyDebug(dTimer, "[\t    func-readPersist-rf(%+v)\t\t] :重新连接啦, 我的日志是:%v,我的voteFor:%v",
		//rf.me, rf.log, rf.votedFor)
		rf.applyIndex = 0
		rf.commitIndex = 0
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	index := -1
	term := -1
	isLeader := rf.getState() == Leader

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}
	if !isLeader {
		return index, term, false
	}
	rf.rw.Lock()
	defer rf.rw.Unlock()
	index = len(rf.log)
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		timeout := rf.getRandTime()
		//fmt.Println(timeout, " ", rf.me)
		select {
		case <-time.After(timeout):
			PrettyDebug(dTimer, "[\t    func-ticker-rf(%+v)\t\t] :选举超时了, 我的身份是:%v,我的voteFor:%v",
				rf.me, rf.state, rf.votedFor)
			rf.electionEvent()
		case <-rf.eventChan:
			PrettyDebug(dTimer, "[\t    func-ticker-rf(%+v)\t\t] :选举刷新了, 我的身份是:%v,我的voteFor:%v",
				rf.me, rf.state, rf.votedFor)
		}
	}
	return
}
func (rf *Raft) heartTicker() {
	for rf.killed() == false {
		select {
		case <-time.After(rf.heartBeatTimeout):
			//PrettyDebug(dTimer, "[\t    func-heartTicker-rf(%+v)\t\t] :心跳刷新了, 我的身份是:%v,我的voteFor:%v",
			//rf.me, rf.state, rf.votedFor)
		case <-rf.heartChan:
			//刷新选举超时

			rf.eventChan <- struct{}{}
		}
	}

}
func (rf *Raft) refreshHeart() {
	rf.heartChan <- struct{}{}
}
func (rf *Raft) refreshElected() {
	rf.eventChan <- struct{}{}
}
func (rf *Raft) getRandTime() time.Duration {
	r := rf.heartTime + rand.Intn(100)
	return time.Duration(r) * time.Millisecond
}

func (rf *Raft) no_op() {
	if rf.killed() {
		return
	}
	rf.refreshHeart()
	//PrettyDebug(dTimer,
	//"[\t    func-no_op-rf(%+v)\t\t] :我在同步日志,我的身份:%v,我的任期:%v",
	//rf.me, rf.state, rf.currentTerm)
	rf.log = append(rf.log, LogEntry{
		Index:   len(rf.log),
		Term:    rf.currentTerm,
		Command: nil,
	})
	voteN := 1
	flag := 1
	for server := range rf.peers {
		if server != rf.me && rf.state == Leader {
			nxtId := rf.nextIndex[server]
			lastLog := rf.log[nxtId-1]
			logs := make([]LogEntry, len(rf.log)-nxtId)
			copy(logs, rf.log[nxtId:])
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: lastLog.Index,
				PrevLogTerm:  lastLog.Term,
				LeaderCommit: rf.commitIndex,
				Entries:      logs,
			}
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, reply) {
				voteN++
				if flag == 1 && voteN > len(rf.peers)/2 {
					flag = 0
					//PrettyDebug(dTimer,
					//"[\t    func-no_op-rf(%+v)\t\t] :同步成功,我的身份:%v,我的任期:%v",
					//rf.me, rf.state, rf.currentTerm)
					go rf.toCommit()
					go rf.applyLog()
				}
			}
		}
	}
	//100秒发送一次心跳吧
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) applyLog() {
	if rf.killed() == false {
		rf.rw.Lock()
		if rf.applyIndex >= rf.commitIndex {
			rf.rw.Unlock()
			return
		}
		commitIndex := rf.commitIndex
		commit := rf.commitIndex
		applied := rf.applyIndex
		entries := make([]LogEntry, commit-applied)
		copy(entries, rf.log[applied+1:commit+1])
		rf.rw.Unlock()
		for _, entry := range entries {
			//PrettyDebug(dTimer, "在这阻塞了，applyLog")
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.rw.Lock()
		rf.applyIndex = commitIndex
		rf.rw.Unlock()
	}
}
func (rf *Raft) toCommit() {
	rf.rw.Lock()
	arr := []int{}
	for i, index := range rf.matchIndex {
		if i == rf.me {
			continue
		}
		arr = append(arr, index)
	}
	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})
	ret := arr[len(arr)/2]
	//本任期内的数据才能提交
	if rf.commitIndex < ret && rf.log[ret].Term == rf.currentTerm {
		//每次提交的时候也要持久化一下
		rf.persist()
		rf.commitIndex = ret
	}

	rf.rw.Unlock()
	go rf.applyLog()
}

func (rf *Raft) min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) errorExit() {
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	// 创建一个通道，在这个例子中用于模拟程序退出
	exit := make(chan bool)

	// 启动一个 goroutine 来监听信号
	go func() {
		// 等待信号
		<-sig
		fmt.Println("收到信号，程序即将退出了:", rf.me)
		// 发送退出信号到 exit 通道
		exit <- true
	}()

	// 等待程序退出
	<-exit
	fmt.Println("程序已退出")

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		rw:               sync.RWMutex{},
		peers:            nil,
		persister:        nil,
		me:               0,
		dead:             0,
		state:            Follower,
		currentTerm:      0,
		votedFor:         -1,
		heartTime:        300,
		commitIndex:      0,
		applyChan:        applyCh,
		applyIndex:       0,
		heartChan:        make(chan struct{}),
		heartBeatTimeout: 120 * time.Millisecond,
		eventChan:        make(chan struct{}),
		voteN:            0,
	}
	a := LogEntry{
		Index:   0,
		Term:    0,
		Command: nil,
	}
	rf.log = []LogEntry{a}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	nextIndex := make([]int, len(peers))
	for i, _ := range nextIndex {
		nextIndex[i] = 1
	}
	rf.nextIndex = nextIndex
	rf.matchIndex = make([]int, len(rf.peers))
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartTicker()
	go rf.errorExit()
	return rf
}
