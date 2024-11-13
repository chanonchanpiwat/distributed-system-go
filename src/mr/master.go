package mr

import (
	"container/heap"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var waitTime = 10 * time.Millisecond

type TaskType int

const (
	MapType TaskType = iota
	ReduceType
	ExitType
)

type TaskStatus string

const (
	NotStarted TaskStatus = "NoStarted"
	Processing TaskStatus = "Processing"
	Completed  TaskStatus = "Completed"
)

func taskStatusOrder(t TaskStatus) int {
	switch t {
	case NotStarted:
		return -1
	case Processing:
		return 0
	case Completed:
		return 1
	default:
		panic("not a task status")
	}
}

type Either[A, B any] struct {
	value any
}

func (e *Either[A, B]) SetA(a A) {
	e.value = a
}

func (e *Either[A, B]) SetB(b B) {
	e.value = b
}

func (e *Either[A, B]) IsLeft() bool {
	_, ok := e.value.(A)
	return ok
}

func (e *Either[A, B]) IsRight() bool {
	_, ok := e.value.(B)
	return ok
}

func Switch[A, B, R any](e *Either[A, B], onA func(A) R, onB func(B) R) R {
	switch v := e.value.(type) {
	case A:
		return onA(v)
	case B:
		return onB(v)
	default:
		panic("Does not implement")
	}
}

type MrTask struct {
	TaskId      int
	Status      TaskStatus
	TaskContent Either[MapTaskArg, ReduceTaskArg]
	index       int
}

func (t MrTask) String() string {
	var taskType string
	if t.TaskContent.IsLeft() {
		taskType = "MapTask"
	} else {
		taskType = "ReduceTask"
	}

	return fmt.Sprintf("task id %d, status: %s, type: %s index: %d\n", t.TaskId, t.Status, taskType, t.index)
}

type TaskHeap []*MrTask

func (h TaskHeap) Peak() *MrTask {
	return h[0]
}

func (h *TaskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1
	*h = old[0 : n-1]
	return item
}

func (h *TaskHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*MrTask)
	item.index = n
	*h = append(*h, item)
}

func (h TaskHeap) Len() int {
	return len(h)
}

func (h TaskHeap) Less(i int, j int) bool {
	left := h[i]
	right := h[j]

	// TO DO: can be simplify
	if left.Status == Completed || right.Status == Completed {
		return taskStatusOrder(left.Status) < taskStatusOrder(right.Status)
	} else if left.TaskContent.IsLeft() && right.TaskContent.IsRight() {
		return true
	} else if left.TaskContent.IsRight() && right.TaskContent.IsLeft() {
		return false
	} else if left.Status != right.Status {
		return taskStatusOrder(left.Status) < taskStatusOrder(right.Status)
	} else {
		return false
	}
}

func (h *TaskHeap) update(item *MrTask, status TaskStatus) {

	item.Status = status
	heap.Fix(h, item.index)
}

func (h TaskHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]

	h[i].index = i
	h[j].index = j
}

type TaskQueue struct {
	tasks TaskHeap
	lock  *sync.Mutex
}

func InitTaskQueue(tasks []*MrTask) TaskQueue {

	hea := &TaskHeap{}
	*hea = tasks
	heap.Init(hea)

	return TaskQueue{
		tasks: *hea,
		lock:  &sync.Mutex{},
	}
}

type TaskResponse struct {
	Task            MrTask
	ShouldWait      bool
	AllFinished     bool
	SuccessCallback func()
	ErrorCallback   func()
}

func (t *TaskQueue) IsFinished() bool {
	task := t.tasks.Peak()
	return task.Status == Completed
}

func (t *TaskQueue) GetTask() TaskResponse {
	t.lock.Lock()
	defer t.lock.Unlock()

	task := t.tasks.Peak()

	if task.Status == NotStarted {
		t.tasks.update(task, Processing)

		onSuccess := func() {
			t.lock.Lock()
			defer t.lock.Unlock()
			fmt.Printf("task id: %d completed\n", task.TaskId)

			t.tasks.update(task, Completed)
		}

		onError := func() {
			t.lock.Lock()
			defer t.lock.Unlock()
			fmt.Printf("task id: %d in-completed\n", task.TaskId)

			t.tasks.update(task, NotStarted)

		}

		return TaskResponse{Task: *task, ShouldWait: false, SuccessCallback: onSuccess, ErrorCallback: onError}

	} else if task.Status == Processing {
		return TaskResponse{ShouldWait: true}
	}

	return TaskResponse{AllFinished: true}
}

func DoJobWithTimeout[T any](job func() T, onSuccess func(), onTimeOut func()) error {

	done := make(chan T, 1)

	go func() {
		done <- job()
	}()

	select {
	case <-done:
		onSuccess()
		return nil
	case <-time.After(waitTime):
		onTimeOut()
		return fmt.Errorf("Timeout")
	}
}

type Queue[T comparable] struct {
	items map[T]bool
	cond  *sync.Cond
}

func (m *Queue[T]) Push(item T) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	m.items[item] = true
	m.cond.Broadcast()
}

func (m Queue[T]) Has(item T) bool {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	for {
		_, ok := m.items[item]
		if ok {
			return true
		}
		m.cond.Wait()
	}
}

type MapTaskArg struct {
	MapId          int
	FileName       string
	NumberOfReduce int
}
type ReduceTaskArg struct {
	ReduceId  int
	ReduceNum int
}

func (m MapTaskArg) String() string {
	return fmt.Sprintf("mapId: %d FileName: %s NumberOfInter: %d", m.MapId, m.FileName, m.NumberOfReduce)
}

func (r ReduceTaskArg) String() string {
	return fmt.Sprintf("reduceId: %d nReduce: %d", r.ReduceId, r.ReduceNum)
}

type Master struct {
	CompletedQueue Queue[int]
	TaskQueue      TaskQueue
}

type RequestTaskArg struct {
	WorkerId int
}

type RequestTaskReply struct {
	Proceed bool
	Wait    bool
	Exit    bool
	Task    MrTask
}

type ReplyTaskArg struct {
	TaskId   int
	TaskType TaskType
	TimeUsed int
}

type ReplyTaskReply struct {
	Acknowledged bool
}

func (m *Master) RequestTask(arg RequestTaskArg, reply *RequestTaskReply) error {

	res := m.TaskQueue.GetTask()

	if res.AllFinished {
		reply.Exit = true
		return nil
	} else if res.ShouldWait {
		reply.Wait = true
		return nil
	}

	reply.Proceed = true
	reply.Task = res.Task

	go func() {
		DoJobWithTimeout(func() bool { return m.CompletedQueue.Has(res.Task.TaskId) }, res.SuccessCallback, res.ErrorCallback)

	}()

	return nil
}

func (m *Master) ReplyTask(arg ReplyTaskArg, reply *ReplyTaskReply) error {

	m.CompletedQueue.Push(arg.TaskId)

	reply.Acknowledged = true
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.TaskQueue.IsFinished()
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	var TaskId int
	mrTasks := make([]*MrTask, 0, len(files)+nReduce)

	for id, fName := range files {
		mapTask := Either[MapTaskArg, ReduceTaskArg]{MapTaskArg{id, fName, nReduce}}
		mrTasks = append(mrTasks, &MrTask{TaskId, NotStarted, mapTask, TaskId})
		TaskId++
	}

	for r := range nReduce {
		reduceTask := Either[MapTaskArg, ReduceTaskArg]{ReduceTaskArg{r, nReduce}}
		mrTasks = append(mrTasks, &MrTask{TaskId, NotStarted, reduceTask, TaskId})
		TaskId++
	}

	m.TaskQueue = InitTaskQueue(mrTasks)
	m.CompletedQueue = Queue[int]{make(map[int]bool), sync.NewCond(&sync.Mutex{})}

	m.server()
	return &m
}
