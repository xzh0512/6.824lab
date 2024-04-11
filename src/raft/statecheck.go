package raft

func (rf *Raft) setState(st Status) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	/*
		switch rf.state {
		case Follower:
			switch st {
			case Follower:
			case Leader:
			case Candidate:
				rf.state = st
				rf.currentTerm++
				rf.votedFor = rf.me
			}
		case Candidate:
			switch st {
			case Follower:
				rf.state = st
				rf.votedFor = -1
			case Leader:
				rf.state = st
			case Candidate:
			}
		case Leader:
			switch st {
			case Follower:
				rf.state = st
				rf.votedFor = -1
			default:
			}
		}
	*/
	rf.state = st
}
func (rf *Raft) getState() Status {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.state
}
