package simulator

import (
	"DES-go/schedulers/types"
	"fmt"
	"math"
)

type GPUJobQueue struct {
	gpu  *GPU
	jobs []*Job
}

func (q *GPUJobQueue) GPU() types.GPU {
	return q.gpu
}

func (q *GPUJobQueue) Jobs() []types.Job {
	jobs := make([]types.Job, 0, len(q.jobs))
	for _, j := range q.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (q *GPUJobQueue) SetJobs(jobs ...types.Job) {
	res := make([]*Job, 0, len(jobs))
	for _, j := range jobs {
		res = append(res, j.(*Job))
	}
	q.jobs = res
	// those jobs not on the first rank cannot have 'running' status
	for i := 1; i < len(q.jobs); i++ {
		q.jobs[i].setNotRunning()
	}
}

func (q *GPUJobQueue) ClearQueue() []types.Job {
	for _, job := range q.jobs {
		job.setNotRunning()
	}
	res := q.Jobs()
	q.jobs = []*Job{}
	return res
}

func NewGPUJobQueue(gpu types.GPU) *GPUJobQueue {
	return &GPUJobQueue{
		gpu:  gpu.(*GPU),
		jobs: make([]*Job, 0),
	}
}

func (q *GPUJobQueue) passDuration(fromTime types.Time, duration types.Duration, flag bool) []*Job {
	maxFinishedIdx := -1
	//minFinishedIdx := len(q.jobs) + 1
	//currTime := fromTime + types.Time(duration)
	tempTime := fromTime
	//finishIdx := 0
	finished := make([]*Job, 0, len(q.jobs))
	finishIdxList := make([]int, 0, len(q.jobs))
	for idx, j := range q.jobs {
		if j.IsFinished() {
			panic(fmt.Sprintf("GPUJobQueue %+v passDuration %+v, j.IsFinished() is true, j = %+v", q, duration, j))
		}
		//j.executesFor(q.gpu, tempTime, types.Duration(currTime-tempTime))
		j.executesFor(q.gpu, tempTime, duration)
		//if !j.IsFinished() {
		//	break
		//}
		if flag == true {
			//finishIdx = idx
			if j.IsFinished() {
				//finishIdx = idx
				finishIdxList = append(finishIdxList, idx)
			}
		}
		maxFinishedIdx = int(math.Max(float64(maxFinishedIdx), float64(idx)))
		//minFinishedIdx = int(math.Min(float64(minFinishedIdx), float64(idx)))
		//tempTime = j.FinishExecutionTime()
	}
	for i := range finishIdxList {
		finished = append(finished, q.jobs[finishIdxList[i]])
		q.jobs = append(q.jobs[:finishIdxList[i]], q.jobs[finishIdxList[i]+1:]...)
	}
	//finished := q.jobs[:maxFinishedIdx+1]
	//q.jobs = q.jobs[maxFinishedIdx+1:]
	//finished := make([]*Job, 0, len(q.jobs))
	//if flag == true {
	//	finished = append(finished, q.jobs[finishIdx])
	//	q.jobs = append(q.jobs[:finishIdx], q.jobs[finishIdx+1:]...)
	//}
	return finished
}

func (q *GPUJobQueue) FirstJobRemainingDuration() types.Duration {
	if len(q.jobs) == 0 {
		return types.Duration(math.Inf(1))
	}
	minDuration := math.Inf(1)
	minIdx := -1
	for idx := range q.jobs {
		if float64(q.jobs[idx].RemainingDuration(q.gpu.Type())) < minDuration {
			minDuration = float64(q.jobs[idx].RemainingDuration(q.gpu.Type()))
			minIdx = idx
		}
	}
	return q.jobs[minIdx].RemainingDuration(q.gpu.Type())
}
