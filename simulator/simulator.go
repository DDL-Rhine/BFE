package simulator

import (
	"DES-go/schedulers/types"
	"fmt"
	"log"
	"math"
)

type Simulator struct {
	opts      *Options
	scheduler types.Scheduler
	cluster   *Cluster
	logger    *logger

	recordedFinishedJobs []types.Job
}

func NewSimulator(scheduler types.Scheduler, setOpts ...SetOption) *Simulator {
	opts := defaultOptions

	for _, setOpt := range setOpts {
		setOpt(opts)
	}
	if opts.dataSourceCSVPath != "" {
		initDataSource(opts.dataSourceCSVPath, opts.dataSourceRange)
	}

	logger := NewLogger(opts.logEnabled, opts.logDirPath)
	return &Simulator{
		scheduler:            scheduler,
		opts:                 opts,
		cluster:              NewCluster(opts.gpuType2Count),
		logger:               logger,
		recordedFinishedJobs: make([]types.Job, 0),
	}
}

func (s *Simulator) Run() *types.Record {
	s.cluster.startServe()
	s.scheduler.SetCluster(s.cluster)
	getDataSource().IterBySubmitTime(func(indices []int, metas []types.JobMeta) {
		submitTime := metas[0].SubmitTime()
		for _, meta := range metas {
			if meta.SubmitTime() != submitTime {
				panic("getDataSource().IterBySubmitTime metas' submit times are different.")
			}
		}

		if float64(submitTime-s.cluster.Now()) < -float64(s.opts.minDurationPassInterval) {
			panic(fmt.Sprintf("meta.submitTime() = %v - s.cluster.Now() = %v) >= -float64(s.opts.minDurationPassInterval = %v)", submitTime, s.cluster.Now(), s.opts.minDurationPassInterval))
		}

		NewWaitingJobs := make([]types.Job, 0, len(metas))
		for _, meta := range metas {
			NewWaitingJobs = append(NewWaitingJobs, NewJob(meta))
		}
		waitingNum := 0
		waitingNum = s.emitEvent(types.NewScheduleEventJobsArrived(metas), 0, waitingNum)
		for s.scheduler.JobListLen() > 0 || !s.cluster.IsJobQueueEmpty() { //s.scheduler.AllFragmented() || !s.cluster.IsJobQueueEmpty()
			//if !s.cluster.IsJobQueueEmpty() {
			closestTimeToFinishAnyJob := s.cluster.ClosestTimeToFinishAnyJob()
			//fmt.Printf("colsestTime:%.2f\n", closestTimeToFinishAnyJob)
			s.scheduler.ReceiveGPU(s.cluster.ClosestGPUToFinishAnyJob())
			/*for s.cluster.Now() < submitTime*/
			//for closestTimeToFinishAnyJob > 0 {
			//passDuration := submitTime - s.cluster.Now()
			//	closestTimeToFinishAnyJob--
			passDuration := closestTimeToFinishAnyJob
			currTime := s.passDuration(types.Duration(passDuration), false, waitingNum)
			//fmt.Printf("cycling...\n")
			//}
			//}
			if s.scheduler.JobListLen() > 0 {
				waitingNum = s.emitEvent(types.NewScheduleEventRestJobsArrived(s.scheduler.WaitingJobs()), currTime, waitingNum)
				//waitingNum = s.emitEvent(types.NewScheduleEventRestJobsArrived(s.scheduler.WaitingJobs()), currTime, -(s.scheduler.JobListLen() + 1))
			}
		}
		//s.emitEvent(types.NewScheduleEventJobsArrived(metas))
	})
	//s.passDuration(0, true)
	return &types.Record{
		SchedulerName:   s.scheduler.Name(),
		SchedulerInfo:   s.scheduler.Info(),
		GPUs:            s.cluster.GPUs(),
		FinishedJobs:    s.recordedFinishedJobs,
		SchedulerRecord: s.scheduler.Record(),
	}
}

func ProcessWaitingJobs(jobs []types.Job) []types.Job {
	return jobs
}

func (s *Simulator) passDuration(duration types.Duration, noMoreNewSubmits bool, waitingNum int) types.Time {
	currTime := s.cluster.Now()
	targetTime := currTime + types.Time(duration)
	if noMoreNewSubmits {
		targetTime = 1e38
	}
	for currTime < targetTime || noMoreNewSubmits {
		closestTimeToFinishAnyJob := s.cluster.ClosestTimeToFinishAnyJob()
		nextActiveScheduleTime := s.scheduler.NextActiveScheduleTime()
		// 如果调度器将不会进行主动调度，并且将来没有任务要完成，并且指定不会再有新的任务提交了，那么此时认为模拟结束了。
		if math.IsInf(float64(nextActiveScheduleTime), 1) &&
			math.IsInf(float64(closestTimeToFinishAnyJob), 1) &&
			noMoreNewSubmits {
			// All jobs done
			return currTime
		}
		// calculate partial time.
		// in case some jobs finish very closely, use max() to specify a min interval.
		// targetTime - currTime is the upper limit.
		possibleNextEventTime := math.Min(float64(s.scheduler.NextActiveScheduleTime()), float64(closestTimeToFinishAnyJob))
		//partialDuration := types.Duration(math.Min(math.Max(possibleNextEventTime, float64(s.opts.minDurationPassInterval)), float64(targetTime-currTime)))
		partialDuration := types.Duration(math.Min(math.Max(possibleNextEventTime, float64(s.opts.minDurationPassInterval)), float64(duration)))
		receiveGPU := s.scheduler.GetReceivedGPU()
		finishedJobs, _ := s.cluster.passDuration(partialDuration, receiveGPU)
		// fmt.Printf("finishedJobs len=[%d], all Finished len=[%d]", len(finishedJobs), len(s.recordedFinishedJobs))
		s.logTimePassed(partialDuration)
		currTime += types.Time(partialDuration)
		for _, job := range finishedJobs {
			s.recordedFinishedJobs = append(s.recordedFinishedJobs, job)
		}
		s.emitEvent(types.NewScheduleEventDurationPassed(partialDuration), currTime, waitingNum)
		//if len(finishedJobs) > 0 {
		//	s.emitEvent(types.NewScheduleEventJobsFinished(s.transformJobs(finishedJobs)))
		//}
	}
	return currTime
}

func (s *Simulator) transformJobs(jobs []*Job) []types.Job {
	res := make([]types.Job, 0, len(jobs))
	for _, job := range jobs {
		res = append(res, job)
	}
	return res
}

func (s *Simulator) logTimePassed(duration types.Duration) {
	if s.opts.formatPrintLevel == AllFormatPrint {
		allInfo := fmt.Sprintf("\nTime Passed: %f seconds, finished jobs count: %d. \ncluster info: \n%v.\n", float64(duration), len(s.recordedFinishedJobs), s.cluster)
		log.Printf(allInfo)
	} else if s.opts.formatPrintLevel == ShortMsgPrint {
		log.Printf("\nTime Passed: %f seconds, finished jobs count: %d.\n", float64(duration), len(s.recordedFinishedJobs))
	} else if s.opts.formatPrintLevel == NoPrint {
		// pass.
	}
}

func (s *Simulator) logJobFinished(finishedJobs []types.Job) {
	if s.opts.formatPrintLevel == AllFormatPrint || s.opts.formatPrintLevel == ShortMsgPrint {
		log.Printf("finishedJobs len=[%d], all Finished len=[%d]\n", len(finishedJobs), len(s.recordedFinishedJobs))
	} else if s.opts.formatPrintLevel == NoPrint {
		// pass.
	}
}

func (s *Simulator) emitEvent(event types.ScheduleEvent, currTime types.Time, waitingNum int) int {
	waitingNum = s.scheduler.OnScheduleEvent(event, currTime, waitingNum)
	//s.scheduler.AllFragmented()
	return waitingNum
}
