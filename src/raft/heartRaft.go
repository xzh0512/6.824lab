package raft

import "time"

type RequestHeartAllArgs struct {
	//当前任期
	Term     int
	LeaderId int
	//上一批预写日志的内容
	PrevTerm  int
	PrevIndex int
}
type RequestHeartAllReply struct {
}

func (rf *Raft) RequestHeartAll(args *RequestHeartAllArgs, reply *RequestHeartAllReply) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	//当前任期小的情况直接就是拒绝票
	if args.Term < rf.currentTerm {
		return
	}
	PrettyDebug(dTimer, "[\t    func-RequestHeartAll-rf(%+v)\t\t] :有人请求同步心跳:%v, 我的身份是:%v,我所在的任期%v",
		rf.me, args.LeaderId, rf.state, rf.currentTerm)
	state := rf.state
	switch state {
	case Follower:
		rf.followerHA(args, reply)
	case Leader:
		rf.leaderHA(args, reply)
	case Candidate:
		rf.candidateHA(args, reply)
	}
	return
}
func (rf *Raft) followerHA(args *RequestHeartAllArgs, reply *RequestHeartAllReply) {
	rf.refreshHeart()
	//不要让他们的投票更新!!!不然后面的相同任期的candidate就会来拉票
	//rf.votedFor = -1
	rf.currentTerm = args.Term
	//判断预写日志是否滞后
	if args.PrevTerm >= rf.log[len(rf.log)-1].Term && args.PrevIndex >= rf.log[len(rf.log)-1].Index {
		//Todo:同步预写日志
		return
	}
}

func (rf *Raft) leaderHA(args *RequestHeartAllArgs, reply *RequestHeartAllReply) {
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.followerHA(args, reply)
	}
}
func (rf *Raft) candidateHA(args *RequestHeartAllArgs, reply *RequestHeartAllReply) {
	if args.Term >= rf.currentTerm {
		rf.state = Follower
		rf.followerHA(args, reply)
	}
}

func (rf *Raft) sendHeartAll() {
	for rf.getState() == Leader {
		if rf.killed() {
			return
		}
		go rf.refreshHeart()
		PrettyDebug(dTimer,
			"[\t    func-sendHeartAll-rf(%+v)\t\t] :我在同步心跳,我的身份:%v,我的任期:%v",
			rf.me, rf.state, rf.currentTerm)

		for i := 0; i < len(rf.peers); i++ {
			//刷新心跳超时
			if i == rf.me {
				continue
			}
			rf.rw.RLock()
			args := RequestHeartAllArgs{
				rf.currentTerm,
				rf.me,
				rf.log[len(rf.log)-1].Term,
				rf.log[len(rf.log)-1].Index,
			}
			rf.rw.RUnlock()
			reply := RequestHeartAllReply{}
			finished := make(chan bool)
			h := time.Now()
			go func(i int) {
				rf.peers[i].Call("Raft.RequestHeartAll", &args, &reply)
				finished <- true
			}(i)
			select {
			case <-finished:
			case <-time.After(800 * time.Microsecond):
				// 函数执行完成
				PrettyDebug(dTimer, "[\t    func-sendHeartAll-rf(%+v)\t\t] :我的身份是:%v,请求服务器:%v超时:%v-800us",
					rf.me, rf.state, i, time.Since(h))
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
