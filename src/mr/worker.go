package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var count int

func Map(filename string, contents string) []KeyValue {
	me := os.Getpid()
	f := fmt.Sprintf("mr-worker-jobcount-%d-%d", me, count)
	count++
	err := ioutil.WriteFile(f, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
	return []KeyValue{KeyValue{"a", "x"}}
}

func Reduce(key string, values []string) string {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		panic(err)
	}
	invocations := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-worker-jobcount") {
			invocations++
		}
	}
	return strconv.Itoa(invocations)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//mapf = Map
	//reducef = Reduce
	//CallExample()
	//worker要向main请求任务,调用本地的什么方法，以及具体采用什么是文件
	//调用map方法和redurce方法
	for {
		task := CallCoordinatorRequest()
		var ty TaskType
		if task == nil {
			ty = WaittingTask
		} else {
			ty = task.TaskTy
		}
		//fmt.Println("work.go 42:任务类型", ty)
		switch ty {
		case MapTask:
			WorkMap(mapf, task)
			WorkDone(task)
		case ReduceTask:
			WorkReduce(reducef, task)
			WorkDone(task)
		case WaittingTask:
			time.Sleep(time.Second)
		case ExitTask:
			time.Sleep(2 * time.Second)
			//fmt.Println("没有要处理的任务 worker.go 60")
			return
		}
	}
}

func WorkDone(task *Job) {
	//通知Coordinator任务完成
	args := task
	reply := ReReply{}
	call("Coordinator.WorkDone", &args, &reply)
	//TODO("如果超时了需要把原来产生的文件都删了，重新计算任务")
	/*if reply{
		//删除本地任务
	}*/
}
func WorkReduce(reducef func(string, []string) string, task *Job) {
	files := task.Task
	var intermediate []KeyValue
	intermediate = readMapF(files)
	ProductRdF(intermediate, reducef, task)
}

func ProductRdF(intermediate []KeyValue, reducef func(string, []string) string, task *Job) {
	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	ofile, _ := os.Create(oname)
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-id.
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}

func readMapF(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		sort.Sort(ByKey(kva))
		file.Close()
	}
	return kva
}

func WorkMap(mapf func(string, string) []KeyValue, task *Job) {
	r := task.ReduceN
	//fmt.Printf("任务类型是:%d,任务是:%s,任务ID:%d\n", task.TaskTy, task.Task, task.TaskID)
	filename := task.Task[0]
	content := GetKV(filename)
	kva := mapf(filename, string(content))
	//总共52个英文字母，生成r个中间文件，然后r读取这些文件
	//用一个数组存储不同的key，数组下标就是key的首字母取模运算
	files := make([][]KeyValue, task.ReduceN)
	//根据reduce工人数量划分字母
	//创建r个数组
	for _, value := range kva {
		//追加的形式写入数组
		files[ihash(value.Key)%r] = append(files[ihash(value.Key)%r], value)
	}
	//持久化存储
	FileAppend(files, task.TaskID)
}
func GetKV(filename string) []byte {
	file, err := os.Open(filename)
	//如果该类型的文件存在就删除原来的再创建一个

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func FileAppend(files [][]KeyValue, id int) {
	for i, file := range files {
		mapfilename := fmt.Sprintf("MapWokerID%d-%d", id, i)
		ofile, _ := os.Create(mapfilename)
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		for _, kv := range file {
			enc.Encode(kv)
		}
	}
}

// 请求任务
func CallCoordinatorRequest() *Job {
	reply := Job{}
	//没有请求参数，参数设置为1
	args := RequestArgs{}
	//返回一个文件列表给worker
	ok := call("Coordinator.Poll", &args, &reply)
	if ok {
		return &reply
	}
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
