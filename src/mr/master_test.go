package mr

import (
	"fmt"
	utils "github.com/chanonchanpiwat/distributed-system-go/src/utils"
	"testing"
	"time"
)

func MakeFilePath() []string {
	files := make([]string, 0, 10)
	for i := range 10 {
		files = append(files, fmt.Sprintf("file %d", i))
	}
	return files
}

func TestMasterOkTask(t *testing.T) {

	master := MakeMaster(MakeFilePath(), 10)

	var task RequestTaskReply
	master.RequestTask(RequestTaskArg{1}, &task)

	var replyTask ReplyTaskReply
	master.ReplyTask(ReplyTaskArg{task.Task.TaskId, MapType, 0}, &replyTask)

	time.Sleep(100 * time.Millisecond)

	utils.Filter(master.TaskQueue.tasks, func(ts *MrTask, _ int) bool {
		if ts.TaskId == task.Task.TaskId && ts.Status != Completed {
			t.Errorf("task id %d must be completed", task.Task.TaskId)
		}
		return true
	})

}

func TestMasterTimeOutTask(t *testing.T) {

	master := MakeMaster(MakeFilePath(), 10)

	var task RequestTaskReply
	master.RequestTask(RequestTaskArg{1}, &task)


	time.Sleep(100 * time.Millisecond)

	utils.Filter(master.TaskQueue.tasks, func(ts *MrTask, _ int) bool {
		if ts.TaskId == task.Task.TaskId && ts.Status != NotStarted{
			t.Errorf("task id %d must be revert to not start", task.Task.TaskId)
		}
		return true
	})

}
