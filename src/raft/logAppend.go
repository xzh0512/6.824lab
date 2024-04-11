package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//前一笔预写的日志（不是提交的）
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// ------------------------------logcopy part------------------------------------//
func (rf *Raft) AppendEntriesLog() {

	//最开始都是发一个no-op
	//rf.no_op()
	for rf.getState() == Leader {
		if rf.killed() {
			//PrettyDebug(dTimer,
			//"[\t    func-AppendEntriesLog-rf(%+v)\t\t] :我被kill了,我的身份:%v,我的任期:%v",
			//rf.me, rf.state, rf.currentTerm)
			return
		}
		//rf.matchIndex[rf.me] = len(rf.log) - 1
		PrettyDebug(dTimer,
			"[\t    func-AppendEntriesLog-rf(%+v)\t\t] :我在同步日志,我的身份:%v,我的任期:%v",
			rf.me, rf.state, rf.currentTerm)
		for server := range rf.peers {
			if server != rf.me && rf.getState() == Leader {
				go rf.sendAppendto(server)
			}
		}
		go rf.refreshHeart()
		//100发送一次心跳吧
		time.Sleep(20 * time.Millisecond)
	}
}
func (rf *Raft) sendAppendto(server int) {
	rf.rw.Lock()
	nxtId := rf.nextIndex[server]
	lastLog := rf.log[nxtId-1]
	logs := make([]LogEntry, len(rf.log[nxtId:]))
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
	rf.rw.Unlock()
	//错误观念：超时了我就下一次再传啊
	//ok := rf.sendAppendEntries(server, args, reply)
	//h := time.Now()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		//这里超时一定要重试？因为我迫切的想要更新commit
		if rf.killed() {
			return
		}
		//ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		//ok = rf.sendAppendEntries(server, args, reply)
		//ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		//PrettyDebug(dTimer, "[\t    func-AppendEntries-rf(S%+v)\t\t] :同步日志超时:S%v", rf.me, server)
		return
	}
	rf.rw.Lock()
	defer rf.rw.Unlock()
	//todo:当前nextIndex已经被改变
	if rf.state != Leader || rf.killed() || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}
	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		go rf.toCommit()
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = reply.Term, -1
		rf.persist()
		rf.state = Follower
	} else if reply.XTerm == -1 {
		rf.nextIndex[server] -= reply.XLen
	} else if reply.XTerm >= 0 {
		termNotExit := true
		for index := rf.nextIndex[server] - 1; index >= 1; index-- {
			entry := rf.log[index]
			if entry.Term > reply.XTerm {
				continue
			}
			if entry.Term == reply.XTerm {
				rf.nextIndex[server] = index + 1
				termNotExit = false
				break
			}
			if entry.Term < reply.XTerm {
				break
			}
		}
		if termNotExit {
			rf.nextIndex[server] = reply.XIndex
		}
	} else {
		/*这个条件不会走*/
		rf.nextIndex[server] = reply.XIndex
		panic("我不会走到这儿！！！！")
	}
	// the smallest nextIndex is 1
	// otherwise, it will cause out of range error
	if rf.nextIndex[server] < 1 {
		rf.nextIndex[server] = 1
	}
}

// 选举的时候只要比较一下日志条目就行
// 成为leader的第一件事就是把自己的任期追加到日志后面去
// 同步日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = false
	//当前任期小的情况直接就是拒绝同步
	PrettyDebug(dTimer, "[\t    func-AppendEntries-rf(%+v)\t\t] :有人请求同步日志:%v, 我的身份是:%v", rf.me, args.LeaderId, rf.state)
	if args.Term < rf.currentTerm {
		return
	}
	state := rf.state
	switch state {
	case Follower:
		rf.followerLA(args, reply)
	case Leader:
		rf.leaderLA(args, reply)
	case Candidate:
		rf.candidateLA(args, reply)
	}
	return
}
func (rf *Raft) followerLA(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	go rf.refreshHeart()
	//能进入到这里说明leader是合法的！！！，所以要跟leader前一笔预写日志列表对应上
	//重置心跳
	//PrettyDebug(dTimer, "[\t    func-followerLA-rf(%+v)\t\t] :有人请求同步日志:%v, 我的身份是:%v", rf.me, args.LeaderId, rf.state)
	//refreshHeart()
	//更新自己的任期
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
	}

	//判断预写日志是否滞后
	//先前日志信息
	curIndex := len(rf.log) - 1
	curLogEntry := rf.log[curIndex]
	//日志一致性检查，有空缺！！妙啊
	//todo:返回空缺列表索引，或者在前一笔任期不存在这个term
	/*if args.PrevLogIndex > curLogEntry.Index || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		return
	}
	*/
	idx := args.PrevLogIndex
	if args.PrevLogIndex > curLogEntry.Index {
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - curLogEntry.Index
		return
	} else if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Success = false
		reply.XTerm = rf.log[idx].Term
		reply.XIndex = args.PrevLogIndex
		// 0 is a dummy entry => quit in index is 1
		for index := idx; index >= 1; index-- {
			if rf.log[index-1].Term != reply.XTerm {
				reply.XIndex = index
				break
			}
		}
		return
	}
	reply.Success = true
	id := args.PrevLogIndex

	//处理重复的RPC
	for i, entry := range args.Entries {
		//要追加的下标号
		id++
		if id < len(rf.log) {
			//已经追加成功了
			if rf.log[id].Term == entry.Term {
				continue
			}
			//后面冗余的直接截断
			rf.log = rf.log[:id]
		}
		//在后面追加即可
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}
	//更新提交列表
	if rf.commitIndex < args.LeaderCommit {
		lastLogIndex := rf.log[len(rf.log)-1].Index
		if args.LeaderCommit > lastLogIndex {
			rf.commitIndex = lastLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.matchIndex[rf.me] = rf.commitIndex
	}
	go rf.applyLog()
	/*PrettyDebug(dTimer, "[\t    func-followerLA-rf(%+v)\t\t] :有人请求同步日志:%v, 我的身份是:%v,commit:%v,LeaderCommit:%v,apply:%v", rf.me, args.LeaderId, rf.state, rf.commitIndex, args.LeaderCommit, rf.applyIndex)
	for _, entry := range rf.log {
		fmt.Printf("%v,", entry)
	}
	fmt.Println()

	*/
}

func (rf *Raft) leaderLA(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.followerLA(args, reply)
	}
}
func (rf *Raft) candidateLA(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//大于等于自己的任期就退回成follow的执行流程
	if args.Term >= rf.currentTerm {
		rf.state = Follower
		rf.followerLA(args, reply)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	finished := make(chan bool)
	go func() {
		rf.peers[server].Call("Raft.AppendEntries", args, reply)
		finished <- true
	}()
	select {
	case <-finished:
		return true
	case <-time.After(1000 * time.Microsecond):
		return false
	}
}
