package mr

import (
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Your code here -- RPC handlers for the worker to call.
var mu sync.Mutex = sync.Mutex{}

type TaskType int

// 枚举任务的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask // Waittingen任务代表此时为任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask     // exit
)

type Phase int

// 枚举阶段的类型
const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

type State int

// 任务状态类型
const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

type JobMeta struct {
	st        State
	starttime time.Time //记录任务启动时间
	jobArr    *Job
}

type Coordinator struct {
	MapCh    chan *Job
	ReduceCh chan *Job
	//Reduce工人数量
	ReduceN int
	//分配mapId
	MapN int
	//存放全部任务信息，以便判断任务是否完成
	JobMetaHolder map[int]*JobMeta
	JobID         int
	MPhase        Phase
	Mchan         chan struct{}
}
type Job struct {
	// Your definitions here.
	//任务列表
	Task []string
	//已经发完的map任务的下一个任务
	TaskID int
	//每次分配多少任务，每次分配一个文件
	TaskTy TaskType
	//reduceWshu
	ReduceN int
}

// MapProduct 生成map任务
func (m *Coordinator) MapProduct(f []string, r int) {
	//将文件一个一个放进管道里面
	for _, j := range f {
		id := m.getid()
		jb := []string{j}
		a := &Job{
			Task:    jb,
			TaskID:  id,
			TaskTy:  MapTask,
			ReduceN: r,
		}
		// 保存任务的初始状态
		taskMetaInfo := &JobMeta{
			st:        Waiting, // 任务等待被执行
			starttime: time.Now(),
			jobArr:    a, // 保存任务的地址
		}
		m.JobMetaHolder[id] = taskMetaInfo
		//fmt.Println("Coordinator.go 117 make a reduce task :", &task)
		m.MapCh <- a
	}

}

func (m *Coordinator) ReduceProduct() {
	<-m.Mchan
	//fmt.Println("Coordinator.go 94:Map worker has done")
	//读取文件列表根据Reduce号
	//将文件一个一个放进管道里面
	for i := 0; i < m.ReduceN; i++ {
		id := m.getid()
		task := &Job{
			TaskID:  id,
			TaskTy:  ReduceTask,
			Task:    selectReduceName(i),
			ReduceN: m.ReduceN,
		}
		// 保存任务的初始状态
		taskMetaInfo := &JobMeta{
			st:        Waiting, // 任务等待被执行
			starttime: time.Now(),
			jobArr:    task, // 保存任务的地址
		}
		mu.Lock()
		m.JobMetaHolder[id] = taskMetaInfo
		//fmt.Println("Coordinator.go 117 make a reduce task :", &task)
		m.ReduceCh <- task
		mu.Unlock()
	}
}

func selectReduceName(i int) []string {
	var s []string
	path, _ := os.Getwd()
	infos, _ := ioutil.ReadDir(path)
	for _, file := range infos {
		if strings.HasPrefix(file.Name(), "MapWokerID") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
			s = append(s, file.Name())
		}
	}
	//fmt.Println(s)
	return s
}

func (m *Coordinator) WorkDone(args *Job, reply *ReReply) error {
	mu.Lock()
	defer mu.Unlock()
	//判断是什么任务
	task := args
	Id := task.TaskID
	//fmt.Println("Coordinator.go 104 任务ID", Id)
	if v, ok := m.JobMetaHolder[Id]; !ok {
		//fmt.Println("任务参数错误")
	} else if v.st == Done {
		//fmt.Println("任务已完成")
	}
	//丢弃该任务
	if time.Since(m.JobMetaHolder[Id].starttime) > 10*time.Second {

	}
	//mu := sync.Mutex{}
	m.JobMetaHolder[Id].st = Done
	return nil
}
func (m *Coordinator) Poll(args *RequestArgs, reply *Job) error {
	var err error
	//fmt.Println("当前阶段", m.MPhase)
	//mu := sync.Mutex{}
	mu.Lock()
	phase := m.MPhase
	mu.Unlock()
	switch phase {
	case MapPhase:
		err = m.Mmap(args, reply)
		//fmt.Println("进入Map阶段")
	case ReducePhase:
		err = m.Mreudce(args, reply)
		//fmt.Println("进入Reduce阶段")
	default:
		reply.TaskTy = ExitTask
		//fmt.Println("所有任务完成")
	}
	return err
}

// start a thread that listens for RPCs from worker.go

// main/mrCoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Coordinator) Done() bool {
	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if m.MPhase == AllDone {
		return true
	}

	return false
}

func (m *Coordinator) getid() int {
	ret := m.JobID
	m.JobID++
	return ret
}

func (m *Coordinator) Mreudce(args *RequestArgs, reply *Job) error {
	//判断是否有任务到来
	//判断是否有任务可以分配,如果当前管道为空就没有任务了
	if len(m.ReduceCh) == 0 {
		//fmt.Println("Coordinator.go 216任务已分配完成,等待reduce处理完成")
		reply.TaskTy = WaittingTask
		if m.checkTaskDone() {
			m.toNextPhase(ReducePhase)
		}
		return nil
	}
	mu.Lock()
	defer mu.Unlock()
	//分配文件名
	a := <-m.ReduceCh
	//如果是超时完成的任务就直接返回
	if m.JobMetaHolder[a.TaskID].st == Done {
		reply.TaskTy = WaittingTask
		return nil
	}
	m.JobMetaHolder[a.TaskID].st = Working
	*reply = *a
	//fmt.Println(a.Task)
	return nil
}

// Mmap
func (m *Coordinator) Mmap(args *RequestArgs, reply *Job) error {
	//判断是否有任务可以分配,如果当前管道为空就没有任务了
	if len(m.MapCh) == 0 {
		//fmt.Println("任务已分配完成,等待map处理完成:Coordinator.go 108")
		reply.TaskTy = WaittingTask
		if m.checkTaskDone() {
			m.toNextPhase(MapPhase)
		}
		return nil
	}
	mu.Lock()
	defer mu.Unlock()
	//分配文件名
	//mu := sync.Mutex{}
	a := <-m.MapCh
	//如果是超时完成的任务就直接返回
	if m.JobMetaHolder[a.TaskID].st == Done {
		reply.TaskTy = WaittingTask
		return nil
	}
	m.JobMetaHolder[a.TaskID].st = Working
	*reply = *a
	//fmt.Println(a.Task)
	return nil
}

func (m *Coordinator) checkTaskDone() bool {
	mu.Lock()
	defer mu.Unlock()
	t := m.JobMetaHolder
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	// 遍历储存task信息的map
	for _, v := range t {
		// 首先判断任务的类型
		if v.jobArr.TaskTy == MapTask {
			// 判断任务是否完成,下同
			if v.st == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.jobArr.TaskTy == ReduceTask {
			if v.st == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}

	}
	//fmt.Printf("Coordinator.go 213:map tasks  are finished %d/%d, reduce task are finished %d/%d \n",
	//mapDoneNum, mapDoneNum+mapUnDoneNum, reduceDoneNum, reduceDoneNum+reduceUnDoneNum)

	// 如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true

	// R
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		//表明map阶段已经完成
		m.Mchan <- struct{}{}
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false
}

func (m *Coordinator) toNextPhase(phase Phase) {
	//question:我的意思是开始两个协程的MPhase都是MapPhase，
	//一个协程先获得锁把共享变量改为ReducePhase，另一个再获得锁，由于判断条件又把它改了AllDone。
	//但我希望只修改一次，怎么避免上述情况发生
	//理解了您的意思，要避免上述情况发生，您可以使用条件变量（sync.Cond）来实现更细粒度的同步。
	mu.Lock()
	switch phase {
	case MapPhase:
		m.MPhase = ReducePhase
	case ReducePhase:
		m.MPhase = AllDone
	default:
	}
	mu.Unlock()

}

// Crash 循环检测是否有任务超时
func (m *Coordinator) Crash() {
	for {
		//mu := sync.Mutex{}
		time.Sleep(2 * time.Second) // 每2s检查一次
		mu.Lock()
		if m.MPhase == AllDone {
			mu.Unlock()
			break
		}
		for _, meta := range m.JobMetaHolder {
			if meta.st == Working {
				//超时了
				if time.Since(meta.starttime) > 10*time.Second {
					meta.st = Waiting
					if meta.jobArr.TaskTy == MapTask {
						m.MapCh <- meta.jobArr
					} else if meta.jobArr.TaskTy == ReduceTask {
						m.ReduceCh <- meta.jobArr
					}
				}
			}
		}
		//fmt.Println("Crash任务执行", time.Now())
		mu.Unlock()
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	m := Coordinator{
		MPhase:        MapPhase,
		MapCh:         make(chan *Job, len(files)),
		ReduceCh:      make(chan *Job, nReduce),
		ReduceN:       nReduce,
		JobMetaHolder: make(map[int]*JobMeta, len(files)+nReduce),
		MapN:          0,
		JobID:         0,
		Mchan:         make(chan struct{}, 1),
	}
	//生成Map任务
	m.MapProduct(files, nReduce)
	// Your code here.
	go m.ReduceProduct()
	go m.Crash()
	m.server()
	return &m
}
