package raft

type RaftLog struct {
	snapLastIdx  int
	snapLastTerm int

	//[1,snapLastIdx]
	snapshot []byte

	//(snapLastIdx,snapLastIdx+len(tailLog)-1]
	//在头部存一个head点snapLastIdx
	tailLog []LogEntry
}

// NewLog 构造函数
func NewLog(snapLsatIdx, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {

}

// 反序列化函数
func (rl *RaftLog) readPersist() {

}

//序列化

// 下标转换函数
func (rl *RaftLog) size() int {

}
func (rl *RaftLog) idx(logicIdx int) int {
	//如果逻辑地址超出了就
	if logicIdx < rl.snapLastIdx || logicIdx > rl.size() {
		panic("逻辑地址超限")
	}
	return logicIdx - rl.snapLastIdx
}
