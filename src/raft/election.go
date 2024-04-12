package raft

// ------------------------------election part------------------------------------//
func (rf *Raft) electionEvent() {
	go rf.refreshElected()
	//心跳超时了发起选举
	PrettyDebug(dTimer, "[	    func-electionEvent-rf(%+v)		] : rf.Term: %v\n", rf.me, rf.currentTerm)
	state := rf.getState()
	switch state {
	//如果是follower会尝试成为候选人，并提高自己的任期，发起新一轮的竞选
	case Follower:
		rf.electionStart()
	//如果已经是candidate会提高自己的任期,发起新一轮的竞选
	case Candidate:
		rf.electionStart()
	//会同步自己的心跳
	case Leader:
		rf.setState(Follower)
		//PrettyDebug(dTimer, "[	    func-electionEvent-rf(%+v)		] : 我先退位了\n", rf.me)
	}

}
func (rf *Raft) electionStart() {
	rf.rw.Lock()
	//把自己变为候选人
	rf.state = Candidate
	//自身任期加加
	rf.currentTerm++
	//为自己投票!!!
	rf.votedFor = rf.me
	rf.persist()
	voteN := 1
	rf.rw.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		//PrettyDebug(dTimer, "[	    func-electionStart-rf(%+v)		] : 循环开始了,选举任期rf.Term: %v,得到票数:%v,为谁投票了%v\n",
		//rf.me, rf.currentTerm, VoteN, rf.votedFor)
		if rf.killed() {
			return
		}
		if i == rf.me {
			continue
		}
		rf.rw.RLock()
		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
			PrevTerm:    rf.log[len(rf.log)-1].Term,
			PrevIndex:   rf.log[len(rf.log)-1].Index,
		}
		rf.rw.RUnlock()
		reply := RequestVoteReply{}
		//不要再协程之前重置心跳会变得不幸
		go rf.sendRequestVote(i, &args, &reply, &voteN)
	}
	//单纯的投票数不通过，主要是超时
	//PrettyDebug(dTimer, "[	    func-electionEvent-rf(%+v)		] : 我选举失败了,选举任期rf.Term: %v,得到票数:%v,为谁投票了%v\n",
	//rf.me, rf.currentTerm, VoteN, rf.votedFor)
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//当前任期号
	Term int
	//节点ID
	CandidateId int
	//上一批预写日志的内容
	PrevTerm  int
	PrevIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	//当前节点最新的任期号
	Term int
	//是否同意
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.rw.Lock()
	defer rf.rw.Unlock()
	defer rf.persist()
	PrettyDebug(dTimer, "[\t    func-RequestVote-rf(S%+v)\t\t] :有人请求我投票:S%v, 我的身份是:%v,我的任期:%v，lastterm:%v,lastlogindex:%v",
		rf.me, args.CandidateId, rf.state, rf.currentTerm, rf.log[len(rf.log)-1].Term, len(rf.log)-1)
	// 当前节点crash
	state := rf.state
	//当前任期小的情况直接就是拒绝票
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm || rf.killed() {
		//fmt.Println("有人请求我投票了,但是我拒绝了:", args.CandidateId)
		return
	}
	switch state {
	case Follower:
		rf.followerRV(args, reply)
	case Leader:
		rf.leaderRV(args, reply)
	case Candidate:
		rf.candidateRV(args, reply)
	}
	return
}
func (rf *Raft) followerRV(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	//已经投票,拒绝本轮相同任期的其他candidate,等他选举超时了,或者遇到任期更大的那个voteFor自然会重置。防止相同任期的抢票！
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	//判断任期是否滞后
	lastIndex := len(rf.log) - 1
	//如果大于竞选者的任期，或者任期相同，index大于竞选者的也会退出
	if args.PrevTerm < rf.log[lastIndex].Term ||
		(args.PrevTerm == rf.log[lastIndex].Term && rf.log[lastIndex].Index > args.PrevIndex) {
		return
	}
	go rf.refreshElected()
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
}
func (rf *Raft) leaderRV(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = Follower
		rf.followerRV(args, reply)
	}
}
func (rf *Raft) candidateRV(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = Follower
		rf.followerRV(args, reply)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteN *int) {
	//PrettyDebug(dTimer, "[\t    func-sendRequestVote-rf(%+v)\t\t] :请求服务器投票阻塞:%v, 我的身份是:%v,我的voteFor:%v",
	//rf.me, server, rf.state, rf.votedFor)
	//h := time.Now()

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.rw.Lock()
	defer rf.rw.Unlock()
	if rf.state != Candidate || rf.killed() || args.Term != rf.currentTerm {
		return
	}
	// 函数执行完成
	PrettyDebug(dTimer, "[\t    func-sendRequestVote-rf(%+v)\t\t] :请求服务器投票回复:%v, 对方是否为我投票:%v",
		rf.me, server, reply.VoteGranted)
	if reply.Term > rf.currentTerm {
		//还要更新一下自己的任期啊
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = Follower
		rf.persist()
		return
	} else if reply.VoteGranted {
		*voteN++
		if *voteN > len(rf.peers)/2 {
			//PrettyDebug(dTimer,
			//"[	    func-electionEvent-rf(%+v)		] : 当选成功:rf.Term: %v\n",
			//rf.me, rf.currentTerm)
			//rf.setState(Leader)
			rf.persist()
			rf.state = Leader
			go rf.refreshHeart()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			//追加no-op日志...
			//更新自己的nextIndex和matchIndex,这样反而不好，会可能至少两次?
			//rf.rw.Lock()
			//rf.rw.Unlock()
			go rf.AppendEntriesLog()
			//go rf.sendHeartAll()
		}
	}

}
