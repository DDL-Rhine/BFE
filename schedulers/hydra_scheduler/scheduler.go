package hydra_scheduler

import (
	"DES-go/schedulers/hydra_scheduler/cost"
	"DES-go/schedulers/jobs_util"
	"DES-go/schedulers/types"
	"DES-go/simulator"
	"DES-go/util"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Scheduler 大体思路：
// 通过类似KMeans聚类的方式，将所有待调度的job分片到每个可用资源上。
// ------------------------------------------------------------------------------------------------------------------------
// 版本1 思路如下：
// 考虑将所有job和GPU放在一个平面进行考虑，将job和GPU都作为点来考虑。
// 初始，将所有的GPU看做KMeans算法中的初始簇中心点。即，有多少个GPU，则K为多大。
// KMeans聚类的目标，即为，将所有的job划分到所有的GPU簇中。
// 考虑每个job到任意一个GPU的距离为，它放入到这个GPU簇之后（最优放置），对整个集群的JCT和DDL带来的伤害（用这种方式可以将soft DDL考虑进来）。
// 则，该伤害越小，距离越近。在每次迭代中，找出那个job对整个集群的伤害最小的。将它放置到簇中。
// 迭代完成后，就得到了针对每个GPU的，一个job的划分。
// ------------------------------------------------------------------------------------------------------------------------
type Scheduler struct {
	gpuCluster types.Cluster
	// waitingJobs 按照任意一个GPU上的速度排序，它的顺序决定了进行算法时的迭代（KMeans聚类时算法）的顺序。（这个顺序会造成怎样的影响，有待商榷）
	waitingJobs []types.Job
	opts        *Options

	allArrivedJobsCount int

	DoScheduleCallRecords []*types.DoScheduleCallRecord

	allFragmented bool

	receivedGPU []int

	originalJobs []types.Job
}

// Options
// 选项模式。动态指定调度器的参数。
type Options struct {
	ScheduleScheme ScheduleScheme
	DistanceAlgo   DistanceAlgo
}

type TargetJob struct {
	TargetJob  types.Job
	Percentage float64
}

type FragAmount struct {
	GpuId int
	Data  []float64
}

const (
	Satisfied    = "satisfied"
	NotSatisfied = "no_satisfied"
)

var FragMap = map[string]int{
	Satisfied:    0,
	NotSatisfied: 1,
}

// 指定默认的选项。
var defaultOptions = &Options{
	ScheduleScheme: NewBasicScheduleScheme(true, false, -1, true),
	DistanceAlgo: NewMinCostDistanceAlgo(
		cost.NewBranchAndBoundAlgo(cost.BranchAndBoundLCStandardPartialCost, cost.BranchAndBoundAlgoTypeAllPermutation),
		cost.NewSimpleAddCostSolverMaker(cost.DDLCostTypeStrict, 1e20)),
}

// SetOptions 选项模式的设值用函数。
type SetOptions func(options *Options)

// WithScheme 指定调度机制。
func WithScheme(scheduleScheme ScheduleScheme) SetOptions {
	return func(options *Options) {
		options.ScheduleScheme = scheduleScheme
	}
}

// WithDistanceAlgo 指定KMeans用于测定距离的算法
func WithDistanceAlgo(distanceAlgo DistanceAlgo) SetOptions {
	return func(options *Options) {
		options.DistanceAlgo = distanceAlgo
	}
}

// New 初始化该调度器。可指定选项参数。
func New(setOptions ...SetOptions) *Scheduler {
	opts := &Options{
		ScheduleScheme: defaultOptions.ScheduleScheme,
		DistanceAlgo:   defaultOptions.DistanceAlgo,
	}
	for _, setOption := range setOptions {
		setOption(opts)
	}
	scheduler := &Scheduler{
		opts:                  opts,
		DoScheduleCallRecords: make([]*types.DoScheduleCallRecord, 0, 128),
	}
	opts.ScheduleScheme.SetScheduler(scheduler)
	return scheduler
}

// DoSchedule 调度入口
func (k *Scheduler) DoSchedule(currTime types.Time, waitingNum int) int {
	start := time.Now()
	validSchedule, waitingNum := k.opts.ScheduleScheme.DoSchedule(currTime, waitingNum)
	duration := time.Since(start)
	if !validSchedule {
		return 0
	}
	k.DoScheduleCallRecords = append(k.DoScheduleCallRecords, &types.DoScheduleCallRecord{
		Duration: duration,
	})
	return waitingNum
}

func (k *Scheduler) SetCluster(cluster types.Cluster) {
	k.gpuCluster = cluster
	k.waitingJobs = make([]types.Job, 0, 1)
}

func (k *Scheduler) insertJobs2Waiting(jobs ...types.Job) {
	// 这里将jobs插入到waitingJobs当中
	// 在这里指定了waitingJobs的排序顺序，也就决定了将来指定层次聚类算法的迭代顺序。
	// 使用随意选择的GPU，按照任务在它上面执行的剩余时间进行排序。（替代方案，可以选用最快的GPU进行排序，毕竟某些任务在慢的GPU上可能差距很小）
	sortedByGPUType := k.gpuCluster.GPUTypes()[0]
	for _, job := range jobs {
		target := job.RemainingDuration(sortedByGPUType)
		i := sort.Search(len(k.waitingJobs), func(i int) bool {
			return k.waitingJobs[i].RemainingDuration(sortedByGPUType) >= target
		})
		k.waitingJobs = jobs_util.GetJobsSliceUtil().InsertJobsSlice(job, i, k.waitingJobs)
		k.originalJobs = append([]types.Job{}, k.waitingJobs...)
	}
}

func (k *Scheduler) OnScheduleEvent(event types.ScheduleEvent, currTime types.Time, waitingNum int) int {
	//var isFinished = false
	switch e := event.(type) {
	case *types.ScheduleEventJobsArrived:
		{
			k.allArrivedJobsCount += len(e.JobMetas())
			newJobs := make([]types.Job, 0, len(e.JobMetas()))
			for _, jobMeta := range e.JobMetas() {
				newJobs = append(newJobs, k.gpuCluster.InitJob(jobMeta))
			}
			k.insertJobs2Waiting(newJobs...)
			/*if len(k.gpuCluster.EmptyGPUJobQueues()) > 0*/
			if !k.gpuCluster.AllFragmented() {
				waitingNum = k.DoSchedule(currTime, waitingNum)
			}
			//return false
		}
	case *types.ScheduleEventDurationPassed:
		{
			// ignore
			/*newJobs := make([]types.Job, 0, len(k.waitingJobs))
			for idx := range k.waitingJobs {
				newJobs = append(newJobs, k.waitingJobs[idx])
			}
			k.insertJobs2Waiting(newJobs...)
			if len(k.gpuCluster.EmptyGPUJobQueues()) > 0 {
				k.DoSchedule()
			}*/
		}
	case *types.ScheduleEventJobsFinished:
		{
			waitingNum = k.DoSchedule(currTime, waitingNum)
		}
	case *types.ScheduleEventRestJobsArrived:
		{
			//newJobs := make([]types.Job, 0, len(k.waitingJobs))
			//for idx := range k.waitingJobs {
			//	newJobs = append(newJobs, k.waitingJobs[idx])
			//}
			//k.insertJobs2Waiting(newJobs...)
			if !k.gpuCluster.AllFragmented() {
				waitingNum = k.DoSchedule(currTime, waitingNum)
			}
		}
	}
	//return isFinished
	return waitingNum
}

func (k *Scheduler) NextActiveScheduleTime() types.Time {
	return types.Time(math.Inf(1))
}

func (k *Scheduler) Name() string {
	return fmt.Sprintf("KMeansScheduler")
}

func (k *Scheduler) Info() interface{} {
	return map[string]interface{}{
		"Type":           "KMeansScheduler",
		"ScheduleScheme": k.opts.ScheduleScheme.String(),
		"DistanceAlgo":   k.opts.DistanceAlgo.String(),
	}
}

func (k *Scheduler) Record() *types.SchedulerRecord {
	return &types.SchedulerRecord{
		DoScheduleRecords: k.DoScheduleCallRecords,
		Extra:             k.opts.ScheduleScheme.RecordExtra(),
	}
}

func (k *Scheduler) AllFragmented() bool {
	return k.allFragmented
}

func (k *Scheduler) ReceiveGPU(gpuId []int) {
	k.receivedGPU = gpuId
}

func (k *Scheduler) JobListLen() int {
	return len(k.waitingJobs)
}

func (k *Scheduler) GetReceivedGPU() []int {
	return k.receivedGPU
}

func (k *Scheduler) WaitingJobs() []types.Job {
	return k.waitingJobs
}

// --- 接下来定义调度的Scheme ---------------------------------------------------------------------------------------------

type ScheduleScheme interface {
	SetScheduler(scheduler *Scheduler)
	// DoSchedule 返回bool确认这次调度是否有效
	DoSchedule(currTime types.Time, waitingNum int) (bool, int)
	String() string
	RecordExtra() interface{}
}

type BasicScheduleScheme struct {
	scheduler *Scheduler
	// 指定是否可抢占，如果在实际中是可抢占的，那么需要指定一个调度的周期（否则会造成每次调度时造成大量的任务启停开销）
	// 如果不指定周期，则为一个理想化的调度（现实中无法实现）
	Preemptive      bool
	Parallel        bool
	PreemptiveCycle types.Duration
	OneShot         bool

	// hasDoneOnceSchedule 在OneShot为false时使用。OneShot为false时，只允许一次调度发生。
	hasDoneOnceSchedule bool

	// distanceSolver
	distanceSolver *jobDistanceSolver

	Record *BasicScheduleSchemeSummaryRecord
}

func NewBasicScheduleScheme(Parallel bool,
	Preemptive bool,
	PreemptiveCycle types.Duration,
	OneShot bool) ScheduleScheme {
	return &BasicScheduleScheme{
		Parallel:        Parallel,
		Preemptive:      Preemptive,
		PreemptiveCycle: PreemptiveCycle,
		OneShot:         OneShot,
		Record: &BasicScheduleSchemeSummaryRecord{
			KMeansRoundDurations: make([]time.Duration, 0, 1024),
		},
	}
}

func (s *BasicScheduleScheme) SetScheduler(scheduler *Scheduler) {
	s.scheduler = scheduler
	s.distanceSolver = newJobDistanceSolver(scheduler.opts.DistanceAlgo)
	s.distanceSolver.scheduler = scheduler
}

func (s *BasicScheduleScheme) String() string {
	return fmt.Sprintf("BasicScheduleScheme[Parallel=%v, OneShot=%v]", s.Parallel, s.OneShot)
}

func (s *BasicScheduleScheme) RecordExtra() interface{} {
	s.Record.AverageKMeansRoundDurationMs = int(util.AvgDuration(s.Record.KMeansRoundDurations...).Milliseconds())
	s.Record.MaxKMeansRoundDurationMs = int(util.MaxDuration(s.Record.KMeansRoundDurations...).Milliseconds())
	s.Record.MinKMeansRoundDurationMs = int(util.MinDuration(s.Record.KMeansRoundDurations...).Milliseconds())
	s.Record.DistanceSolverRecordExtra = s.distanceSolver.RecordExtra()
	return s.Record
}

type BasicScheduleSchemeSummaryRecord struct {
	KMeansRoundDurations         []time.Duration `json:"-"`
	AverageKMeansRoundDurationMs int             `json:"average_k_means_round_duration_ms"`
	MaxKMeansRoundDurationMs     int             `json:"max_k_means_round_duration_ms"`
	MinKMeansRoundDurationMs     int             `json:"min_k_means_round_duration_ms"`
	DistanceSolverRecordExtra    interface{}     `json:"distance_solver_record_extra"`
}

// DoSchedule 简单的one shot机制。将全部jobs一次性使用KMeans得出分配结果。
func (s *BasicScheduleScheme) DoSchedule(currTime types.Time, waitingNum int) (bool, int) {
	scheduler := s.scheduler
	if s.OneShot && s.hasDoneOnceSchedule {
		return false, 0
	}
	// 初始，先将每个GPU放到簇中。
	// 每个簇中的jobs使用slice存储，但是一般时刻不关心它的顺序。
	// 只有最优解的顺序是我们关心的。所以每次将最优解的job sequence赋值到簇中。
	kMeansCluster := make(map[types.GPU][]types.Job)
	for gpuID, v := range scheduler.gpuCluster.GPUJobQueues() {
		gpu := scheduler.gpuCluster.GPU(gpuID)
		kMeansCluster[gpu] = make([]types.Job, 0)
		kMeansCluster[gpu] = v.Jobs()
	}
	//for len(scheduler.waitingJobs) > 0 {
	_, waitingNum = s.FillKMeansCluster(scheduler, kMeansCluster, currTime, waitingNum)
	s.SetKMeansResult2GPUCluster(scheduler, kMeansCluster)
	//}
	return true, waitingNum
}

func (s *BasicScheduleScheme) SetKMeansResult2GPUCluster(scheduler *Scheduler, kMeansCluster map[types.GPU][]types.Job) {
	for gpuID, queue := range scheduler.gpuCluster.GPUJobQueues() {
		//if int(gpuID) == scheduler.receivedGPU {
		gpu := scheduler.gpuCluster.GPU(gpuID)
		// 找到空闲的队列。
		//if len(queue.Jobs()) == 0 {
		if len(kMeansCluster[gpu]) > 0 {
			if !s.OneShot {
				// 将该GPU簇对应的最优序列的第一个任务放置到空闲位置上。
				var removed types.Job
				removed, kMeansCluster[gpu] = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(0, kMeansCluster[gpu])
				//gpu.(*simulator.GPU).MilliGpu += removed.MilliGpu()
				queue.SetJobs(removed)
			} else {
				//allJobs := make([]types.Job, len(kMeansCluster[gpu]))
				//copy(allJobs, kMeansCluster[gpu])
				//kMeansCluster[gpu] = kMeansCluster[gpu][0:0]
				//queue.SetJobs(allJobs...)
				var removed types.Job
				queueJob := make([]types.Job, 0, len(kMeansCluster[gpu]))
				for len(kMeansCluster[gpu]) > 0 {
					removed, kMeansCluster[gpu] = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(0, kMeansCluster[gpu])
					//gpu.(*simulator.GPU).MilliGpu += removed.MilliGpu()
					queueJob = append(queueJob, removed)
					//queue.SetJobs(removed)
					//	if gpu.MilliGpuLeft() < removed.MilliGpu() {
					//		break
					//	}
				}
				queue.SetJobs(queueJob...)
			}
		}
		//}
		//} else {
		//	continue
		//}
	}
	// 将剩余的没有调度到GPU上的job重新放置到waitingJobs列表中。
	for _, jobs := range kMeansCluster {
		scheduler.insertJobs2Waiting(jobs...)
	}
	if len(s.scheduler.waitingJobs) == 0 {
		s.hasDoneOnceSchedule = true
	}
}

func (s *BasicScheduleScheme) FillKMeansCluster(scheduler *Scheduler, kMeansCluster map[types.GPU][]types.Job, currTime types.Time, waitingNum int) (map[types.GPU][]types.Job, int) {
	kMeansRoundsDurations := make([]time.Duration, 0, len(scheduler.waitingJobs))
	//for len(scheduler.waitingJobs) > 0 {
	start := time.Now()
	//var bestJobIdx int
	//var bestGPU types.GPU
	//var bestJobsSeq []types.Job
	//var isFinished = false
	if s.Parallel {
		//bestJobIdx, bestGPU, bestJobsSeq = s.KMeansRoundInParallel(scheduler, kMeansCluster)
		kMeansCluster, _, waitingNum = s.KMeansRoundInSerial6(scheduler, kMeansCluster, currTime, waitingNum)
		//kMeansCluster = s.KMeansRoundInParallel3(scheduler, kMeansCluster)
	} else {
		//bestJobIdx, bestGPU, bestJobsSeq = s.KMeansRoundInSerial(scheduler, kMeansCluster)
		//kMeansCluster = s.KMeansRoundInSerial(scheduler, kMeansCluster)
	}
	//kMeansCluster[bestGPU] = bestJobsSeq
	//_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(bestJobIdx, scheduler.waitingJobs)
	duration := time.Since(start)
	kMeansRoundsDurations = append(kMeansRoundsDurations, duration)
	fmt.Printf("kMeans round finished, waitingJobsLength = %3d\n", len(scheduler.waitingJobs))
	//}
	s.Record.KMeansRoundDurations = append(s.Record.KMeansRoundDurations, kMeansRoundsDurations...)
	return kMeansCluster, waitingNum
	//return isFinished
}

func (s *Scheduler) ReceiveFlag(allFragmented bool) {
	s.allFragmented = allFragmented
}
func (s *BasicScheduleScheme) KMeansRoundInSerial6(scheduler *Scheduler, kMeansCluster map[types.GPU][]types.Job, currTime types.Time, waitingNum int) (map[types.GPU][]types.Job, bool, int) {
	var bestGPU types.GPU = nil
	//var bestGPU1 types.GPU = nil
	//var k int64 = 0
	//gpus := make([]types.GPU, 0, len(kMeansCluster))
	//for gpu := range kMeansCluster {
	//	gpus = append(gpus, gpu)
	//}
	//original_gpus := make([]types.GPU, 0, len(kMeansCluster))
	//for gpu := range kMeansCluster {
	//	original_gpus = append(original_gpus, gpu)
	//}

	//waitingJobs := make([]types.Job, 0, len(scheduler.waitingJobs))
	//for job := range scheduler.waitingJobs {
	//	waitingJobs = append(waitingJobs, scheduler.waitingJobs[job])
	//}
	var waitingJobLen = len(scheduler.waitingJobs)
	var isFinished = false
	var originalJobs = make([]types.Job, 0, len(scheduler.waitingJobs))
	for i := range scheduler.waitingJobs {
		originalJobs = append(originalJobs, scheduler.waitingJobs[i])
	}
	sort.Slice(originalJobs, func(i, j int) bool {
		return originalJobs[i].MilliGpu() > originalJobs[j].MilliGpu()
	})
	/*for _, waitingJob := range scheduler.waitingJobs*/
	for _, waitingJob := range originalJobs { //scheduler.waitingJobs
		//k++
		//fmt.Printf("idx:%d", idx)
		waitingJob := waitingJob
		waitingJob.SetWaitingNum(int64(waitingNum))
		minTime := math.Inf(1)
		gpuTypes := scheduler.gpuCluster.GPUTypes()
		var bestGPUType types.GPUType
		for gpu := range gpuTypes {
			if float64(waitingJob.RemainingDuration(gpuTypes[gpu])) < minTime {
				minTime = float64(waitingJob.RemainingDuration(gpuTypes[gpu]))
				bestGPUType = gpuTypes[gpu]
			}
		}
		//waitingJob := scheduler.waitingJobs[0]
		//waitingJobs := scheduler.waitingJobs[k:]
		//waitingJobs = append(waitingJobs[:k], waitingJobs[k+1:]...)
		//gpus := original_gpus
		gpus := make([]types.GPU, 0, len(kMeansCluster))
		gpus1 := make([]types.GPU, 0, len(kMeansCluster))
		gpus2 := make([]types.GPU, 0, len(kMeansCluster))
		//copy(gpus, original_gpus)
		for gpu := range kMeansCluster {
			//if gpu.Type() == bestGPUType {
			gpus = append(gpus, gpu)
			//}
		}
		for gpu := range kMeansCluster {
			if gpu.Type() == bestGPUType {
				gpus1 = append(gpus1, gpu)
			}
		}
		for gpu := range kMeansCluster {
			if gpu.Type() != bestGPUType {
				gpus2 = append(gpus2, gpu)
			}
		}
		for l := 0; l < len(gpus1); { //gpus
			if gpus1[l].MilliGpuLeft() < waitingJob.MilliGpu() {
				gpus1 = append(gpus1[:l], gpus1[l+1:]...)
			} else {
				l++
			}
		}
		for l := 0; l < len(gpus2); { //gpus
			if gpus2[l].MilliGpuLeft() < waitingJob.MilliGpu() {
				gpus2 = append(gpus2[:l], gpus2[l+1:]...)
			} else {
				l++
			}
		}
		for l := 0; l < len(gpus); { //gpus
			if gpus[l].MilliGpuLeft() < waitingJob.MilliGpu() {
				gpus = append(gpus[:l], gpus[l+1:]...)
			} else {
				l++
			}
		}
		//for l := 0; l < len(gpus1); { //gpus
		//	if gpus1[l].MilliGpuLeft() < waitingJob.MilliGpu() {
		//		gpus1 = append(gpus1[:l], gpus1[l+1:]...)
		//	} else {
		//		l++
		//	}
		//}
		//jobList := make([]TargetJob, len(scheduler.waitingJobs)) //waitingJobs
		//for l := range scheduler.waitingJobs {
		//	jobList[l] = TargetJob{
		//		targetJob:  scheduler.waitingJobs[l],
		//		percentage: 1 / float64(waitingJobLen),
		//	}
		//}
		//if len(gpus) != 0 {
		//	globalBestPosition := rand.Intn(len(gpus))
		//	globalBestEnergy := s.distanceSolver.Distance(gpus[globalBestPosition], kMeansCluster[gpus[globalBestPosition]], waitingJob)
		//	globalBestFragScore, _ := calculateFrag(gpus[globalBestPosition], waitingJob, jobList)
		//}
		//if len(gpus1) != 0 {
		//	globalBestPosition1 := rand.Intn(len(gpus1))
		//	globalBestEnergy1 := s.distanceSolver.Distance(gpus1[globalBestPosition1], kMeansCluster[gpus1[globalBestPosition1]], waitingJob)
		//	globalBestFragScore1, _ := calculateFrag(gpus1[globalBestPosition1], waitingJob, jobList)
		//}
		//if len(scheduler.waitingJobs) <= 40 && waitingNum == 0 {
		//	//waitingJobs := scheduler.waitingJobs[k:]
		//	//waitingJobs = append(waitingJobs[:k], waitingJobs[k+1:]...)
		//	//_, waitingJobs := jobs_util.GetJobsSliceUtil().RemoveJobsSlice(int(k), scheduler.waitingJobs)
		//	//if len(scheduler.waitingJobs) > 0 {
		//	//_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(k, scheduler.waitingJobs)
		//	//_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSliceByName(string(waitingJob.JobName()), scheduler.waitingJobs)
		//	//} else {
		//	//	break
		//	//}
		//	//_, waitingJobs := jobs_util.GetJobsSliceUtil().RemoveJobsSlice(int(k), scheduler.waitingJobs)
		//
		//	rand.Seed(time.Now().UnixNano())
		//
		//	// PSO参数设置
		//	numParticles := 30
		//	numIterations := 200 // 增加迭代次数
		//	initialInertiaWeight := 0.9
		//	finalInertiaWeight := 0.4
		//	cognitiveCoeff := 1.5
		//	socialCoeff := 1.5
		//
		//	//-----------------新加的内容--------------
		//	//initialCognitiveCoeff := 2.0
		//	//finalCognitiveCoeff := 0.5
		//	//initialSocialCoeff := 0.5
		//	//finalSocialCoeff := 2.0
		//	//----------------------------------------
		//
		//	//var jobList []TargetJob
		//	//*******************************************************************
		//	jobList := make([]TargetJob, len(scheduler.waitingJobs)) //waitingJobs
		//	for l := range scheduler.waitingJobs {
		//		jobList[l] = TargetJob{
		//			TargetJob:  scheduler.waitingJobs[l],
		//			Percentage: 1 / float64(waitingJobLen),
		//		}
		//	}
		//	//*******************************************************************
		//	// 初始化粒子群
		//	particles := make([]int, numParticles)
		//	velocities := make([]float64, numParticles)
		//	bestPositions := make([]int, numParticles)
		//	//----------------------------------------------------------------
		//	//var globalBestPosition int //--
		//	//l := rand.Intn(len(gpus))
		//	//for gpus[l].MilliGpuLeft() < waitingJob.MilliGpu() { //--
		//	//	globalBestPosition = l
		//	//} //--
		//	//----------------------------------------------------------------
		//	//*******************************************************************
		//	globalBestPosition := rand.Intn(len(gpus))
		//	globalBestEnergy := s.distanceSolver.Distance(gpus[globalBestPosition], kMeansCluster[gpus[globalBestPosition]], waitingJob, scheduler.originalJobs, currTime)
		//	globalBestFragScore, _ := CalculateFrag(gpus[globalBestPosition], waitingJob, jobList)
		//	//*******************************************************************
		//
		//	for i := range particles {
		//		//l = rand.Intn(len(gpus))                             //--
		//		//for gpus[l].MilliGpuLeft() < waitingJob.MilliGpu() { //--
		//		//	particles[i] = l
		//		//} //--
		//		particles[i] = rand.Intn(len(gpus))
		//		velocities[i] = rand.Float64()*2 - 1 // 随机速度
		//		bestPositions[i] = particles[i]
		//		energy := s.distanceSolver.Distance(gpus[particles[i]], kMeansCluster[gpus[particles[i]]], waitingJob, scheduler.originalJobs, currTime)
		//		fragScore, _ := CalculateFrag(gpus[particles[i]], waitingJob, jobList)
		//		/*if energy.distance/float64(fragScore) < globalBestEnergy.distance/float64(globalBestFragScore)*/
		//		if energy.distance < globalBestEnergy.distance {
		//			globalBestPosition = particles[i]
		//			globalBestEnergy = energy
		//			globalBestFragScore = fragScore
		//		}
		//	}
		//
		//	// 迭代更新粒子群
		//	for iter := 0; iter < numIterations; iter++ {
		//		// 动态调整惯性权重
		//		inertiaWeight := initialInertiaWeight - (initialInertiaWeight-finalInertiaWeight)*float64(iter)/float64(numIterations)
		//
		//		//-----------------------新加的内容--------------------------
		//		//cognitiveCoeff := initialCognitiveCoeff - (initialCognitiveCoeff-finalCognitiveCoeff)*float64(iter)/float64(numIterations)
		//		//socialCoeff := initialSocialCoeff + (finalSocialCoeff-initialSocialCoeff)*float64(iter)/float64(numIterations)
		//		//----------------------------------------------------------
		//
		//		for i := range particles {
		//			r1 := rand.Float64()
		//			r2 := rand.Float64()
		//
		//			// 更新速度
		//			velocities[i] = inertiaWeight*velocities[i] +
		//				cognitiveCoeff*r1*float64(bestPositions[i]-particles[i]) +
		//				socialCoeff*r2*float64(globalBestPosition-particles[i])
		//
		//			// 设置最大速度限制
		//			v_max := float64(len(gpus)) * 0.1 // 根据需要调整最大速度值
		//			if velocities[i] > v_max {
		//				velocities[i] = v_max
		//			} else if velocities[i] < -v_max {
		//				velocities[i] = -v_max
		//			}
		//
		//			// 更新位置
		//			//for gpus[particles[i]+int(velocities[i])].MilliGpuLeft() < waitingJob.MilliGpu() {
		//			//	particles[i] += int(velocities[i])
		//			//}
		//			particles[i] += int(velocities[i])
		//			if particles[i] < 0 {
		//				particles[i] = 0
		//			}
		//			if particles[i] >= len(gpus) {
		//				particles[i] = len(gpus) - 1
		//			}
		//
		//			// 计算新位置的能量
		//			energy := s.distanceSolver.Distance(gpus[particles[i]], kMeansCluster[gpus[particles[i]]], waitingJob, scheduler.originalJobs, currTime)
		//			fragScore, _ := CalculateFrag(gpus[particles[i]], waitingJob, jobList)
		//			//bestFragScore, _ := calculateFrag(gpus[bestPositions[i]], waitingJob, jobList)
		//			/*if energy.distance/float64(fragScore) < s.distanceSolver.Distance(gpus[bestPositions[i]], kMeansCluster[gpus[bestPositions[i]]], waitingJob).distance/float64(bestFragScore)*/
		//			if energy.distance < s.distanceSolver.Distance(gpus[bestPositions[i]], kMeansCluster[gpus[bestPositions[i]]], waitingJob, scheduler.originalJobs, currTime).distance {
		//				bestPositions[i] = particles[i]
		//			}
		//
		//			/*if energy.distance/float64(fragScore) < globalBestEnergy.distance/float64(globalBestFragScore)*/
		//			if energy.distance < globalBestEnergy.distance {
		//				globalBestPosition = particles[i]
		//				globalBestEnergy = energy
		//				globalBestFragScore = fragScore
		//			}
		//		}
		//
		//		//---------------------------新加的内容---------------------------------
		//		// 局部搜索
		//		/*for i := 0; i < numParticles; i++ {
		//			if particles[i] == globalBestPosition {
		//				continue
		//			}
		//			localBestPosition := particles[i]
		//			localBestEnergy := s.distanceSolver.Distance(gpus[localBestPosition], kMeansCluster[gpus[localBestPosition]], waitingJob)
		//			for j := 0; j < 10; j++ { // 局部搜索步数
		//				newPosition := (particles[i] + rand.Intn(3) - 1) % len(gpus)
		//				if newPosition < 0 {
		//					newPosition += len(gpus)
		//				}
		//				newEnergy := s.distanceSolver.Distance(gpus[newPosition], kMeansCluster[gpus[newPosition]], waitingJob)
		//				if newEnergy.distance < localBestEnergy.distance {
		//					localBestPosition = newPosition
		//					localBestEnergy = newEnergy
		//				}
		//			}
		//			if localBestEnergy.distance < globalBestEnergy.distance {
		//				globalBestPosition = localBestPosition
		//				globalBestEnergy = localBestEnergy
		//			}
		//		}*/
		//		//-----------------------------------------------------------------
		//
		//		// 重新初始化一些粒子的位置，以增加多样性
		//		for i := 0; i < numParticles/10; i++ {
		//			particles[rand.Intn(numParticles)] = rand.Intn(len(gpus))
		//		}
		//	}
		//
		//	bestGPU = gpus[globalBestPosition]
		//	kMeansCluster[bestGPU] = append(kMeansCluster[bestGPU], waitingJob)
		//	testScore := globalBestFragScore
		//	SubGpuMilli(bestGPU, waitingJob)
		//	waitingJob.SetCurrGPUMilli(bestGPU.MilliGpuLeft())
		//	//***********
		//	waitingJob.SetSelectedGpu(bestGPU)
		//	//***********
		//	waitingJob.SetWaitingNum(int64(waitingNum))
		//	waitingNum++
		//	_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSliceByName(string(waitingJob.JobName()), scheduler.waitingJobs)
		//	fmt.Printf("globalBestFrag:%d\n", testScore)
		//	fmt.Printf("globalBestEnergy:%f\n", globalBestEnergy.distance)
		//	fmt.Printf("choose bestGPU\n")
		//	isFinished = false
		//	//}
		//}
		if len(gpus1) != 0 && waitingJob.RemainingDuration(gpuTypes[0]) > 10000 {
			//waitingJobs := scheduler.waitingJobs[k:]
			//waitingJobs = append(waitingJobs[:k], waitingJobs[k+1:]...)
			//_, waitingJobs := jobs_util.GetJobsSliceUtil().RemoveJobsSlice(int(k), scheduler.waitingJobs)
			//if len(scheduler.waitingJobs) > 0 {
			//_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(k, scheduler.waitingJobs)
			//_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSliceByName(string(waitingJob.JobName()), scheduler.waitingJobs)
			//} else {
			//	break
			//}
			//_, waitingJobs := jobs_util.GetJobsSliceUtil().RemoveJobsSlice(int(k), scheduler.waitingJobs)

			rand.Seed(time.Now().UnixNano())

			// PSO参数设置
			numParticles := 30
			numIterations := 200 // 增加迭代次数
			initialInertiaWeight := 0.9
			finalInertiaWeight := 0.4
			cognitiveCoeff := 1.5
			socialCoeff := 1.5

			//-----------------新加的内容--------------
			//initialCognitiveCoeff := 2.0
			//finalCognitiveCoeff := 0.5
			//initialSocialCoeff := 0.5
			//finalSocialCoeff := 2.0
			//----------------------------------------

			//var jobList []TargetJob
			//*******************************************************************
			jobList := make([]TargetJob, len(scheduler.waitingJobs)) //waitingJobs
			for l := range scheduler.waitingJobs {
				jobList[l] = TargetJob{
					TargetJob:  scheduler.waitingJobs[l],
					Percentage: 1 / float64(waitingJobLen),
				}
			}
			//*******************************************************************
			// 初始化粒子群
			particles := make([]int, numParticles)
			velocities := make([]float64, numParticles)
			bestPositions := make([]int, numParticles)
			//----------------------------------------------------------------
			//var globalBestPosition int //--
			//l := rand.Intn(len(gpus))
			//for gpus[l].MilliGpuLeft() < waitingJob.MilliGpu() { //--
			//	globalBestPosition = l
			//} //--
			//----------------------------------------------------------------
			//*******************************************************************
			globalBestPosition := rand.Intn(len(gpus1))
			globalBestEnergy := s.distanceSolver.Distance(gpus1[globalBestPosition], kMeansCluster[gpus1[globalBestPosition]], waitingJob, scheduler.originalJobs, currTime)
			globalBestFragScore, _ := CalculateFrag(gpus1[globalBestPosition], waitingJob, jobList)
			//*******************************************************************

			for i := range particles {
				//l = rand.Intn(len(gpus))                             //--
				//for gpus[l].MilliGpuLeft() < waitingJob.MilliGpu() { //--
				//	particles[i] = l
				//} //--
				particles[i] = rand.Intn(len(gpus1))
				velocities[i] = rand.Float64()*2 - 1 // 随机速度
				bestPositions[i] = particles[i]
				energy := s.distanceSolver.Distance(gpus1[particles[i]], kMeansCluster[gpus1[particles[i]]], waitingJob, scheduler.originalJobs, currTime)
				fragScore, _ := CalculateFrag(gpus1[particles[i]], waitingJob, jobList)
				/*if energy.distance/float64(fragScore) < globalBestEnergy.distance/float64(globalBestFragScore)*/
				if energy.distance < globalBestEnergy.distance {
					globalBestPosition = particles[i]
					globalBestEnergy = energy
					globalBestFragScore = fragScore
				}
			}

			// 迭代更新粒子群
			for iter := 0; iter < numIterations; iter++ {
				// 动态调整惯性权重
				inertiaWeight := initialInertiaWeight - (initialInertiaWeight-finalInertiaWeight)*float64(iter)/float64(numIterations)

				//-----------------------新加的内容--------------------------
				//cognitiveCoeff := initialCognitiveCoeff - (initialCognitiveCoeff-finalCognitiveCoeff)*float64(iter)/float64(numIterations)
				//socialCoeff := initialSocialCoeff + (finalSocialCoeff-initialSocialCoeff)*float64(iter)/float64(numIterations)
				//----------------------------------------------------------

				for i := range particles {
					r1 := rand.Float64()
					r2 := rand.Float64()

					// 更新速度
					velocities[i] = inertiaWeight*velocities[i] +
						cognitiveCoeff*r1*float64(bestPositions[i]-particles[i]) +
						socialCoeff*r2*float64(globalBestPosition-particles[i])

					// 设置最大速度限制
					v_max := float64(len(gpus1)) * 0.1 // 根据需要调整最大速度值
					if velocities[i] > v_max {
						velocities[i] = v_max
					} else if velocities[i] < -v_max {
						velocities[i] = -v_max
					}

					// 更新位置
					//for gpus[particles[i]+int(velocities[i])].MilliGpuLeft() < waitingJob.MilliGpu() {
					//	particles[i] += int(velocities[i])
					//}
					particles[i] += int(velocities[i])
					if particles[i] < 0 {
						particles[i] = 0
					}
					if particles[i] >= len(gpus1) {
						particles[i] = len(gpus1) - 1
					}

					// 计算新位置的能量
					energy := s.distanceSolver.Distance(gpus1[particles[i]], kMeansCluster[gpus1[particles[i]]], waitingJob, scheduler.originalJobs, currTime)
					fragScore, _ := CalculateFrag(gpus1[particles[i]], waitingJob, jobList)
					//bestFragScore, _ := calculateFrag(gpus[bestPositions[i]], waitingJob, jobList)
					/*if energy.distance/float64(fragScore) < s.distanceSolver.Distance(gpus[bestPositions[i]], kMeansCluster[gpus[bestPositions[i]]], waitingJob).distance/float64(bestFragScore)*/
					if energy.distance < s.distanceSolver.Distance(gpus1[bestPositions[i]], kMeansCluster[gpus1[bestPositions[i]]], waitingJob, scheduler.originalJobs, currTime).distance {
						bestPositions[i] = particles[i]
					}

					/*if energy.distance/float64(fragScore) < globalBestEnergy.distance/float64(globalBestFragScore)*/
					if energy.distance < globalBestEnergy.distance {
						globalBestPosition = particles[i]
						globalBestEnergy = energy
						globalBestFragScore = fragScore
					}
				}

				//---------------------------新加的内容---------------------------------
				// 局部搜索
				/*for i := 0; i < numParticles; i++ {
					if particles[i] == globalBestPosition {
						continue
					}
					localBestPosition := particles[i]
					localBestEnergy := s.distanceSolver.Distance(gpus[localBestPosition], kMeansCluster[gpus[localBestPosition]], waitingJob)
					for j := 0; j < 10; j++ { // 局部搜索步数
						newPosition := (particles[i] + rand.Intn(3) - 1) % len(gpus)
						if newPosition < 0 {
							newPosition += len(gpus)
						}
						newEnergy := s.distanceSolver.Distance(gpus[newPosition], kMeansCluster[gpus[newPosition]], waitingJob)
						if newEnergy.distance < localBestEnergy.distance {
							localBestPosition = newPosition
							localBestEnergy = newEnergy
						}
					}
					if localBestEnergy.distance < globalBestEnergy.distance {
						globalBestPosition = localBestPosition
						globalBestEnergy = localBestEnergy
					}
				}*/
				//-----------------------------------------------------------------

				// 重新初始化一些粒子的位置，以增加多样性
				for i := 0; i < numParticles/10; i++ {
					particles[rand.Intn(numParticles)] = rand.Intn(len(gpus1))
				}
			}

			bestGPU = gpus1[globalBestPosition]
			kMeansCluster[bestGPU] = append(kMeansCluster[bestGPU], waitingJob)
			testScore := globalBestFragScore
			SubGpuMilli(bestGPU, waitingJob)
			waitingJob.SetCurrGPUMilli(bestGPU.MilliGpuLeft())
			//***********
			waitingJob.SetSelectedGpu(bestGPU)
			//***********
			waitingJob.SetWaitingNum(int64(waitingNum))
			waitingNum++
			_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSliceByName(string(waitingJob.JobName()), scheduler.waitingJobs)
			fmt.Printf("globalBestFrag:%d\n", testScore)
			fmt.Printf("globalBestEnergy:%f\n", globalBestEnergy.distance)
			fmt.Printf("choose bestGPU\n")
			isFinished = false
			//}
		} else if len(gpus1) != 0 && waitingJob.RemainingDuration(gpuTypes[0]) > 3500 {
			//if len(gpus1) != 0 {
			//if len(gpus1) != 0 {
			//waitingJobs := scheduler.waitingJobs[k:]
			//waitingJobs = append(waitingJobs[:k], waitingJobs[k+1:]...)
			//_, waitingJobs := jobs_util.GetJobsSliceUtil().RemoveJobsSlice(int(k), scheduler.waitingJobs)
			//if len(scheduler.waitingJobs) > 0 {
			//_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(k, scheduler.waitingJobs)
			//_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSliceByName(string(waitingJob.JobName()), scheduler.waitingJobs)
			//} else {
			//	break
			//}
			//_, waitingJobs := jobs_util.GetJobsSliceUtil().RemoveJobsSlice(int(k), scheduler.waitingJobs)

			rand.Seed(time.Now().UnixNano())

			// PSO参数设置
			numParticles := 30
			numIterations := 200 // 增加迭代次数
			initialInertiaWeight := 0.9
			finalInertiaWeight := 0.4
			cognitiveCoeff := 1.5
			socialCoeff := 1.5

			//-----------------新加的内容--------------
			//initialCognitiveCoeff := 2.0
			//finalCognitiveCoeff := 0.5
			//initialSocialCoeff := 0.5
			//finalSocialCoeff := 2.0
			//----------------------------------------

			//var jobList []TargetJob
			//*******************************************************************
			jobList := make([]TargetJob, len(scheduler.waitingJobs)) //waitingJobs
			for l := range scheduler.waitingJobs {
				jobList[l] = TargetJob{
					TargetJob:  scheduler.waitingJobs[l],
					Percentage: 1 / float64(waitingJobLen),
				}
			}
			//*******************************************************************
			// 初始化粒子群
			particles := make([]int, numParticles)
			velocities := make([]float64, numParticles)
			bestPositions := make([]int, numParticles)
			//----------------------------------------------------------------
			//var globalBestPosition int //--
			//l := rand.Intn(len(gpus))
			//for gpus[l].MilliGpuLeft() < waitingJob.MilliGpu() { //--
			//	globalBestPosition = l
			//} //--
			//----------------------------------------------------------------
			//*******************************************************************
			globalBestPosition := rand.Intn(len(gpus))
			globalBestEnergy := s.distanceSolver.Distance(gpus[globalBestPosition], kMeansCluster[gpus[globalBestPosition]], waitingJob, scheduler.originalJobs, currTime)
			globalBestFragScore, _ := CalculateFrag(gpus[globalBestPosition], waitingJob, jobList)
			//*******************************************************************

			for i := range particles {
				//l = rand.Intn(len(gpus))                             //--
				//for gpus[l].MilliGpuLeft() < waitingJob.MilliGpu() { //--
				//	particles[i] = l
				//} //--
				particles[i] = rand.Intn(len(gpus))
				velocities[i] = rand.Float64()*2 - 1 // 随机速度
				bestPositions[i] = particles[i]
				energy := s.distanceSolver.Distance(gpus[particles[i]], kMeansCluster[gpus[particles[i]]], waitingJob, scheduler.originalJobs, currTime)
				fragScore, _ := CalculateFrag(gpus[particles[i]], waitingJob, jobList)
				/*if energy.distance/float64(fragScore) < globalBestEnergy.distance/float64(globalBestFragScore)*/
				if energy.distance < globalBestEnergy.distance {
					globalBestPosition = particles[i]
					globalBestEnergy = energy
					globalBestFragScore = fragScore
				}
			}

			// 迭代更新粒子群
			for iter := 0; iter < numIterations; iter++ {
				// 动态调整惯性权重
				inertiaWeight := initialInertiaWeight - (initialInertiaWeight-finalInertiaWeight)*float64(iter)/float64(numIterations)

				//-----------------------新加的内容--------------------------
				//cognitiveCoeff := initialCognitiveCoeff - (initialCognitiveCoeff-finalCognitiveCoeff)*float64(iter)/float64(numIterations)
				//socialCoeff := initialSocialCoeff + (finalSocialCoeff-initialSocialCoeff)*float64(iter)/float64(numIterations)
				//----------------------------------------------------------

				for i := range particles {
					r1 := rand.Float64()
					r2 := rand.Float64()

					// 更新速度
					velocities[i] = inertiaWeight*velocities[i] +
						cognitiveCoeff*r1*float64(bestPositions[i]-particles[i]) +
						socialCoeff*r2*float64(globalBestPosition-particles[i])

					// 设置最大速度限制
					v_max := float64(len(gpus)) * 0.1 // 根据需要调整最大速度值
					if velocities[i] > v_max {
						velocities[i] = v_max
					} else if velocities[i] < -v_max {
						velocities[i] = -v_max
					}

					// 更新位置
					//for gpus[particles[i]+int(velocities[i])].MilliGpuLeft() < waitingJob.MilliGpu() {
					//	particles[i] += int(velocities[i])
					//}
					particles[i] += int(velocities[i])
					if particles[i] < 0 {
						particles[i] = 0
					}
					if particles[i] >= len(gpus) {
						particles[i] = len(gpus) - 1
					}

					// 计算新位置的能量
					energy := s.distanceSolver.Distance(gpus[particles[i]], kMeansCluster[gpus[particles[i]]], waitingJob, scheduler.originalJobs, currTime)
					fragScore, _ := CalculateFrag(gpus[particles[i]], waitingJob, jobList)
					//bestFragScore, _ := calculateFrag(gpus[bestPositions[i]], waitingJob, jobList)
					/*if energy.distance/float64(fragScore) < s.distanceSolver.Distance(gpus[bestPositions[i]], kMeansCluster[gpus[bestPositions[i]]], waitingJob).distance/float64(bestFragScore)*/
					if energy.distance < s.distanceSolver.Distance(gpus[bestPositions[i]], kMeansCluster[gpus[bestPositions[i]]], waitingJob, scheduler.originalJobs, currTime).distance {
						bestPositions[i] = particles[i]
					}

					/*if energy.distance/float64(fragScore) < globalBestEnergy.distance/float64(globalBestFragScore)*/
					if energy.distance < globalBestEnergy.distance {
						globalBestPosition = particles[i]
						globalBestEnergy = energy
						globalBestFragScore = fragScore
					}
				}

				//---------------------------新加的内容---------------------------------
				// 局部搜索
				/*for i := 0; i < numParticles; i++ {
					if particles[i] == globalBestPosition {
						continue
					}
					localBestPosition := particles[i]
					localBestEnergy := s.distanceSolver.Distance(gpus[localBestPosition], kMeansCluster[gpus[localBestPosition]], waitingJob)
					for j := 0; j < 10; j++ { // 局部搜索步数
						newPosition := (particles[i] + rand.Intn(3) - 1) % len(gpus)
						if newPosition < 0 {
							newPosition += len(gpus)
						}
						newEnergy := s.distanceSolver.Distance(gpus[newPosition], kMeansCluster[gpus[newPosition]], waitingJob)
						if newEnergy.distance < localBestEnergy.distance {
							localBestPosition = newPosition
							localBestEnergy = newEnergy
						}
					}
					if localBestEnergy.distance < globalBestEnergy.distance {
						globalBestPosition = localBestPosition
						globalBestEnergy = localBestEnergy
					}
				}*/
				//-----------------------------------------------------------------

				// 重新初始化一些粒子的位置，以增加多样性
				for i := 0; i < numParticles/10; i++ {
					particles[rand.Intn(numParticles)] = rand.Intn(len(gpus))
				}
			}

			bestGPU = gpus[globalBestPosition]
			kMeansCluster[bestGPU] = append(kMeansCluster[bestGPU], waitingJob)
			testScore := globalBestFragScore
			SubGpuMilli(bestGPU, waitingJob)
			waitingJob.SetCurrGPUMilli(bestGPU.MilliGpuLeft())
			//***********
			waitingJob.SetSelectedGpu(bestGPU)
			//***********
			waitingJob.SetWaitingNum(int64(waitingNum))
			waitingNum++
			_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSliceByName(string(waitingJob.JobName()), scheduler.waitingJobs)
			fmt.Printf("globalBestFrag:%d\n", testScore)
			fmt.Printf("globalBestEnergy:%f\n", globalBestEnergy.distance)
			fmt.Printf("choose bestGPU\n")
			isFinished = false
			//}
		} else if len(gpus2) != 0 && waitingJob.RemainingDuration(gpuTypes[0]) < 3500 {
			//waitingJobs := scheduler.waitingJobs[k:]
			//waitingJobs = append(waitingJobs[:k], waitingJobs[k+1:]...)
			//_, waitingJobs := jobs_util.GetJobsSliceUtil().RemoveJobsSlice(int(k), scheduler.waitingJobs)
			//if len(scheduler.waitingJobs) > 0 {
			//_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSlice(k, scheduler.waitingJobs)
			//_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSliceByName(string(waitingJob.JobName()), scheduler.waitingJobs)
			//} else {
			//	break
			//}
			//_, waitingJobs := jobs_util.GetJobsSliceUtil().RemoveJobsSlice(int(k), scheduler.waitingJobs)

			rand.Seed(time.Now().UnixNano())

			// PSO参数设置
			numParticles := 30
			numIterations := 200 // 增加迭代次数
			initialInertiaWeight := 0.9
			finalInertiaWeight := 0.4
			cognitiveCoeff := 1.5
			socialCoeff := 1.5

			//-----------------新加的内容--------------
			//initialCognitiveCoeff := 2.0
			//finalCognitiveCoeff := 0.5
			//initialSocialCoeff := 0.5
			//finalSocialCoeff := 2.0
			//----------------------------------------

			//var jobList []TargetJob
			//*******************************************************************
			jobList := make([]TargetJob, len(scheduler.waitingJobs)) //waitingJobs
			for l := range scheduler.waitingJobs {
				jobList[l] = TargetJob{
					TargetJob:  scheduler.waitingJobs[l],
					Percentage: 1 / float64(waitingJobLen),
				}
			}
			//*******************************************************************
			// 初始化粒子群
			particles := make([]int, numParticles)
			velocities := make([]float64, numParticles)
			bestPositions := make([]int, numParticles)
			//----------------------------------------------------------------
			//var globalBestPosition int //--
			//l := rand.Intn(len(gpus))
			//for gpus[l].MilliGpuLeft() < waitingJob.MilliGpu() { //--
			//	globalBestPosition = l
			//} //--
			//----------------------------------------------------------------
			//*******************************************************************
			globalBestPosition := rand.Intn(len(gpus2))
			globalBestEnergy := s.distanceSolver.Distance(gpus2[globalBestPosition], kMeansCluster[gpus2[globalBestPosition]], waitingJob, scheduler.originalJobs, currTime)
			globalBestFragScore, _ := CalculateFrag(gpus2[globalBestPosition], waitingJob, jobList)
			//*******************************************************************

			for i := range particles {
				//l = rand.Intn(len(gpus))                             //--
				//for gpus[l].MilliGpuLeft() < waitingJob.MilliGpu() { //--
				//	particles[i] = l
				//} //--
				particles[i] = rand.Intn(len(gpus2))
				velocities[i] = rand.Float64()*2 - 1 // 随机速度
				bestPositions[i] = particles[i]
				energy := s.distanceSolver.Distance(gpus2[particles[i]], kMeansCluster[gpus2[particles[i]]], waitingJob, scheduler.originalJobs, currTime)
				fragScore, _ := CalculateFrag(gpus2[particles[i]], waitingJob, jobList)
				/*if energy.distance/float64(fragScore) < globalBestEnergy.distance/float64(globalBestFragScore)*/
				if energy.distance < globalBestEnergy.distance {
					globalBestPosition = particles[i]
					globalBestEnergy = energy
					globalBestFragScore = fragScore
				}
			}

			// 迭代更新粒子群
			for iter := 0; iter < numIterations; iter++ {
				// 动态调整惯性权重
				inertiaWeight := initialInertiaWeight - (initialInertiaWeight-finalInertiaWeight)*float64(iter)/float64(numIterations)

				//-----------------------新加的内容--------------------------
				//cognitiveCoeff := initialCognitiveCoeff - (initialCognitiveCoeff-finalCognitiveCoeff)*float64(iter)/float64(numIterations)
				//socialCoeff := initialSocialCoeff + (finalSocialCoeff-initialSocialCoeff)*float64(iter)/float64(numIterations)
				//----------------------------------------------------------

				for i := range particles {
					r1 := rand.Float64()
					r2 := rand.Float64()

					// 更新速度
					velocities[i] = inertiaWeight*velocities[i] +
						cognitiveCoeff*r1*float64(bestPositions[i]-particles[i]) +
						socialCoeff*r2*float64(globalBestPosition-particles[i])

					// 设置最大速度限制
					v_max := float64(len(gpus2)) * 0.1 // 根据需要调整最大速度值
					if velocities[i] > v_max {
						velocities[i] = v_max
					} else if velocities[i] < -v_max {
						velocities[i] = -v_max
					}

					// 更新位置
					//for gpus[particles[i]+int(velocities[i])].MilliGpuLeft() < waitingJob.MilliGpu() {
					//	particles[i] += int(velocities[i])
					//}
					particles[i] += int(velocities[i])
					if particles[i] < 0 {
						particles[i] = 0
					}
					if particles[i] >= len(gpus2) {
						particles[i] = len(gpus2) - 1
					}

					// 计算新位置的能量
					energy := s.distanceSolver.Distance(gpus2[particles[i]], kMeansCluster[gpus2[particles[i]]], waitingJob, scheduler.originalJobs, currTime)
					fragScore, _ := CalculateFrag(gpus2[particles[i]], waitingJob, jobList)
					//bestFragScore, _ := calculateFrag(gpus[bestPositions[i]], waitingJob, jobList)
					/*if energy.distance/float64(fragScore) < s.distanceSolver.Distance(gpus[bestPositions[i]], kMeansCluster[gpus[bestPositions[i]]], waitingJob).distance/float64(bestFragScore)*/
					if energy.distance < s.distanceSolver.Distance(gpus2[bestPositions[i]], kMeansCluster[gpus2[bestPositions[i]]], waitingJob, scheduler.originalJobs, currTime).distance {
						bestPositions[i] = particles[i]
					}

					/*if energy.distance/float64(fragScore) < globalBestEnergy.distance/float64(globalBestFragScore)*/
					if energy.distance < globalBestEnergy.distance {
						globalBestPosition = particles[i]
						globalBestEnergy = energy
						globalBestFragScore = fragScore
					}
				}

				//---------------------------新加的内容---------------------------------
				// 局部搜索
				/*for i := 0; i < numParticles; i++ {
					if particles[i] == globalBestPosition {
						continue
					}
					localBestPosition := particles[i]
					localBestEnergy := s.distanceSolver.Distance(gpus[localBestPosition], kMeansCluster[gpus[localBestPosition]], waitingJob)
					for j := 0; j < 10; j++ { // 局部搜索步数
						newPosition := (particles[i] + rand.Intn(3) - 1) % len(gpus)
						if newPosition < 0 {
							newPosition += len(gpus)
						}
						newEnergy := s.distanceSolver.Distance(gpus[newPosition], kMeansCluster[gpus[newPosition]], waitingJob)
						if newEnergy.distance < localBestEnergy.distance {
							localBestPosition = newPosition
							localBestEnergy = newEnergy
						}
					}
					if localBestEnergy.distance < globalBestEnergy.distance {
						globalBestPosition = localBestPosition
						globalBestEnergy = localBestEnergy
					}
				}*/
				//-----------------------------------------------------------------

				// 重新初始化一些粒子的位置，以增加多样性
				for i := 0; i < numParticles/10; i++ {
					particles[rand.Intn(numParticles)] = rand.Intn(len(gpus2))
				}
			}

			bestGPU = gpus2[globalBestPosition]
			kMeansCluster[bestGPU] = append(kMeansCluster[bestGPU], waitingJob)
			testScore := globalBestFragScore
			SubGpuMilli(bestGPU, waitingJob)
			waitingJob.SetCurrGPUMilli(bestGPU.MilliGpuLeft())
			//***********
			waitingJob.SetSelectedGpu(bestGPU)
			//***********
			waitingJob.SetWaitingNum(int64(waitingNum))
			waitingNum++
			_, scheduler.waitingJobs = jobs_util.GetJobsSliceUtil().RemoveJobsSliceByName(string(waitingJob.JobName()), scheduler.waitingJobs)
			fmt.Printf("globalBestFrag:%d\n", testScore)
			fmt.Printf("globalBestEnergy:%f\n", globalBestEnergy.distance)
			fmt.Printf("choose bestGPU\n")
			isFinished = false
			//}
		} else {
			fmt.Printf("best GPU fragmented\n")
			isFinished = true
			continue
		}
		//k++
	}
	scheduler.ReceiveFlag(isFinished)
	//if len(scheduler.waitingJobs) > 0 {
	//	isFinished = false
	//} else {
	//	isFinished = true
	//}
	return kMeansCluster, isFinished, waitingNum
}

func BubbleSort(jobs []types.Job, gpu types.GPU) []types.Job {
	n := len(jobs)
	for i := 0; i < n; i++ {
		for j := 0; j < n-i-1; j++ {
			if jobs[j].RemainingDuration(gpu.Type()) > jobs[j+1].RemainingDuration(gpu.Type()) {
				jobs[j], jobs[j+1] = jobs[j+1], jobs[j]
			}
		}
	}
	return jobs
}

// ---------------------------------------------------------------------------------------------------------------------
// --- 以下部分定义了算法的核心内容，即如何确定一个簇和一个点之间的距离。

// jobDistanceSolver
// 本结构用来计算一批任务在某个价值函数下的放置顺序。
// 可以实现为包含多种算法，比如最优的放置可以使用分支限界搜索方法。
// 如果速度较慢可以使用启发式的贪心算法。
type jobDistanceSolver struct {
	scheduler *Scheduler
	// 避免重复计算，使用memo记录重复参数的调用。
	distanceMemo *sync.Map // map[string]*distanceResp
	distanceAlgo DistanceAlgo

	Record   *DistanceSolverRecordSummaryExtra
	RecordMu *sync.Mutex
}

type DistanceSolverRecordSummaryExtra struct {
	MemorizedCallCount      int         `json:"memorized_call_count"`
	NonMemorizedCallCount   int         `json:"non_memorized_call_count"`
	CallCount               int         `json:"call_count"`
	DistanceAlgoRecordExtra interface{} `json:"distance_algo_record_extra"`
}

type DistanceAlgo interface {
	Distance(gpuCluster types.Cluster,
		kMeansCenterGPU types.GPU,
		kMeansPointJobs []types.Job,
		jobNotInKMeansCluster types.Job,
		waitingJobs []types.Job,
		currTime types.Time) *distanceResp
	String() string
	RecordExtra() interface{}
}

func newJobDistanceSolver(algo DistanceAlgo) *jobDistanceSolver {
	return &jobDistanceSolver{
		scheduler:    nil,         // 等待SetScheduler被调用时注入进来。
		distanceMemo: &sync.Map{}, // make(map[string]*distanceResp),
		distanceAlgo: algo,
		Record:       &DistanceSolverRecordSummaryExtra{},
		RecordMu:     &sync.Mutex{},
	}
}

// distanceMemoKey 为Distance调用生成一个key。用于区分相同的调用。
func (s *jobDistanceSolver) distanceMemoKey(kMeansCenterGPU types.GPU, kMeansPointJobs []types.Job, jobNotInKMeansCluster types.Job) string {
	// 计算距离时，不关心已经在簇里的jobs的顺序，所以需要先按照固定顺序排序。
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(kMeansCenterGPU.Type(), kMeansPointJobs)
	builder := &strings.Builder{}
	// gpu info
	builder.WriteString("GPU:")
	builder.WriteString(kMeansCenterGPU.String())
	writeJob := func(job types.Job) {
		builder.WriteString(string(job.JobName()))
		builder.WriteString(strconv.FormatFloat(job.RemainingRatio(), 'f', 6, 64))
		builder.WriteByte('-')
	}
	if runningJob := s.scheduler.gpuCluster.CurrRunningJob(kMeansCenterGPU.ID()); runningJob != nil {
		builder.WriteString("Running:")
		writeJob(runningJob)
	}
	builder.WriteString("InCluster:")
	for _, job := range kMeansPointJobs {
		writeJob(job)
	}
	builder.WriteString("jobNotInKMeansCluster:")
	writeJob(jobNotInKMeansCluster)
	return builder.String()
}

type distanceResp struct {
	jobsSeq  []types.Job
	distance float64
}

type DistanceCallRecord struct {
	GPUName                  string        `json:"gpu_name"`
	KMeansPointsJobsCount    int           `json:"k_means_points_jobs_count"`
	UseMemorized             bool          `json:"memorized"`
	DistanceAlgoCallDuration time.Duration `json:"distance_algo_call_duration"`
}

// Distance 定义算法入口
// Scheduler 通过使用该方法，获得某个job到某簇的距离
func (s *jobDistanceSolver) Distance(kMeansCenterGPU types.GPU, kMeansPointJobs []types.Job, jobNotInKMeansCluster types.Job, waitingJobs []types.Job, currTime types.Time) *distanceResp {
	record := &DistanceCallRecord{
		GPUName:               kMeansCenterGPU.String(),
		KMeansPointsJobsCount: len(kMeansPointJobs),
	}
	locked := func(f func()) {
		s.RecordMu.Lock()
		defer s.RecordMu.Unlock()
		f()
	}
	defer locked(func() {
		s.Record.CallCount++
	})
	copiedSlice := jobs_util.GetJobsSliceUtil().Copy(kMeansPointJobs)
	memoKey := s.distanceMemoKey(kMeansCenterGPU, copiedSlice, jobNotInKMeansCluster)
	//if memorized, ok := s.distanceMemo.Load(memoKey); ok {
	//	record.UseMemorized = true
	//	return memorized.(*distanceResp)
	//}
	start := time.Now()
	distanceResp := s.distanceAlgo.Distance(s.scheduler.gpuCluster, kMeansCenterGPU, copiedSlice, jobNotInKMeansCluster, waitingJobs, currTime)
	duration := time.Since(start)
	s.distanceMemo.Store(memoKey, distanceResp)

	record.UseMemorized = false
	record.DistanceAlgoCallDuration = duration
	locked(func() {
		s.Record.NonMemorizedCallCount++
	})
	return distanceResp
}

func (s *jobDistanceSolver) RecordExtra() interface{} {
	s.Record.MemorizedCallCount = s.Record.CallCount - s.Record.NonMemorizedCallCount
	s.Record.DistanceAlgoRecordExtra = s.distanceAlgo.RecordExtra()
	return s.Record
}

// ------------------------------------------------ Distance具体算法 ----------------------------------------------------

type MinCostDistanceAlgo struct {
	minCostAlgo     cost.MinCostAlgo
	costSolverMaker cost.SolverMaker

	Record   *MinCostDistanceAlgoSummaryRecord
	RecordMu *sync.Mutex
}

func (m *MinCostDistanceAlgo) String() string {
	return fmt.Sprintf("MinCostDistanceAlgo[minCostAlgo=%s]", m.minCostAlgo)
}

type MinCostDistanceAlgoSummaryRecord struct {
	// CallRecords                   *MinCostDistanceCallRecord `json:"-"`
	CallCount                     int           `json:"call_count"`
	UseMinCostAlgoCount           int           `json:"use_min_cost_algo_count"`
	UseSJFGreedyCount             int           `json:"use_sjf_greedy_count"`
	SumMinCostAlgoDuration        time.Duration `json:"-"`
	AverageMinCostAlgoDurationsMs int           `json:"average_min_cost_algo_durations_ms"`

	MinCostAlgoRecordExtra interface{} `json:"min_cost_algo_record_extra"`
}

type MinCostDistanceCallRecord struct {
	CenterGPUName       string        `json:"center_gpu_name"`
	PointJobsCount      int           `json:"point_jobs_count"`
	UseMinCostAlgo      bool          `json:"use_min_cost_algo"`
	MinCostAlgoDuration time.Duration `json:"min_cost_algo_duration"`
}

// Distance 使用最小cost作为距离的算法参数
// 使用将新的任务放置到该簇后的minCost作为距离。
// 首先，将任务经过SJF排序，如果没有任何job违反ddl，则这个放置的cost就会是最优的，不需要经过后续过程。
// 如果，有任务违反了ddl，则进行分支限界法搜索。
// 使用MinCost顺序选择开节点，每个节点的估计成本为：将所有未放置到搜索路径的任务按照SJF排序后，该队列的代价值。
func (m *MinCostDistanceAlgo) Distance(gpuCluster types.Cluster, kMeansCenterGPU types.GPU, kMeansPointJobs []types.Job, jobNotInKMeansCluster types.Job, waitingJobs []types.Job, currTime types.Time) *distanceResp {
	locked := func(f func()) {
		m.RecordMu.Lock()
		defer m.RecordMu.Unlock()
		f()
	}
	defer locked(func() {
		m.Record.CallCount++
	})
	// 不关心一个簇中任务的顺序。
	jobs := append(kMeansPointJobs, jobNotInKMeansCluster)
	// 首先尝试将jobs使用SRTF排序，并计算一次cost。如果发现ddl没有被违反，则使用这个排序即可。
	//（实际上，算法之所以在总体上不是最优的（由于NP-hard，且不知道任务的到来，所以算不出最优），
	// 也是由于在不违反ddl时，只能使用SJF去思考，无法预测将来的任务到来是否会打散当前的SJF排序。
	// 这是一种贪心的思想。不过只要无法预测将来任务的到来，就不可能做出最优解。）
	// 不过是否可以再用一个度量指标，用于描述这个job有多么容易违反ddl？（离违反ddl有多近）这可以作为之后的改进思路。
	jobs_util.GetJobsSliceUtil().ReorderToSRTF(kMeansCenterGPU.Type(), jobs)
	costSolver := m.costSolverMaker(func(gpu types.GPU) types.Time {
		jctOffset := gpuCluster.Now()
		// 考虑到非抢占式调度，要将当前正在运行的任务剩余运行时间考虑进来。
		runningJob := gpuCluster.CurrRunningJob(gpu.ID())
		if runningJob != nil {
			jctOffset += types.Time(runningJob.RemainingDuration(gpu.Type()))
		}
		return jctOffset
	})
	costResp := costSolver.Cost(kMeansCenterGPU, jobs, waitingJobs, currTime)
	if !costResp.DDLViolated {
		return &distanceResp{
			jobsSeq:  jobs,
			distance: costResp.Cost,
		}
	}
	start := time.Now()
	minCost, optimus := m.minCostAlgo.MinCost(&cost.MinCostParams{
		CostSolver:  costSolver,
		GPU:         kMeansCenterGPU,
		Jobs:        jobs,
		WaitingJobs: waitingJobs,
		CurrTime:    currTime,
	})
	duration := time.Since(start)
	locked(func() {
		m.Record.UseMinCostAlgoCount++
		m.Record.SumMinCostAlgoDuration += duration
	})
	return &distanceResp{
		jobsSeq:  optimus,
		distance: minCost,
	}
}

func NewMinCostDistanceAlgo(minCostAlgo cost.MinCostAlgo, costSolverMaker cost.SolverMaker) DistanceAlgo {
	return &MinCostDistanceAlgo{
		minCostAlgo:     minCostAlgo,
		costSolverMaker: costSolverMaker,
		Record: &MinCostDistanceAlgoSummaryRecord{
			CallCount:              0,
			UseMinCostAlgoCount:    0,
			UseSJFGreedyCount:      0,
			SumMinCostAlgoDuration: 0,
		},
		RecordMu: &sync.Mutex{},
	}
}

func (m *MinCostDistanceAlgo) RecordExtra() interface{} {
	m.Record.UseSJFGreedyCount = m.Record.CallCount - m.Record.UseMinCostAlgoCount
	if m.Record.UseMinCostAlgoCount > 0 {
		m.Record.AverageMinCostAlgoDurationsMs = int(m.Record.SumMinCostAlgoDuration.Milliseconds() / int64(m.Record.UseMinCostAlgoCount))
	}
	m.Record.MinCostAlgoRecordExtra = m.minCostAlgo.RecordExtra()
	return m.Record
}

// ------------------------------------------------ 贪心算法 ------------------------------------------------------------

type SimpleHeuristicGreedyDistanceAlgo struct {
}

func (s *SimpleHeuristicGreedyDistanceAlgo) String() string {
	panic("implement me")
}

func (s *SimpleHeuristicGreedyDistanceAlgo) Distance(gpuCluster types.Cluster, kMeansCenterGPU types.GPU, kMeansPointJobs []types.Job, jobNotInKMeansCluster types.Job) *distanceResp {
	panic("Implement Me.")
}

func (m *SimpleHeuristicGreedyDistanceAlgo) RecordExtra() interface{} {
	return nil
}

func CalculateFrag(gpu types.GPU, job types.Job, jobList []TargetJob) (score int64, gpuId int64) {
	gpuFragScore := GpuFragAmountScore(gpu, jobList)
	score, gpuId = 0, -1
	if gpu.MilliGpuLeft() >= job.MilliGpu() {
		//newGpu := gpu.Copy()
		gpu.(*simulator.GPU).MilliGpu -= job.MilliGpu()
		newGpuFragScore := GpuFragAmountScore(gpu, jobList)
		gpu.(*simulator.GPU).MilliGpu += job.MilliGpu()
		fragScore := int64(sigmoid((gpuFragScore-newGpuFragScore)/1000) * 100)
		if gpuId == -1 || fragScore > score {
			score = fragScore
			gpuId = int64(gpu.ID())
		}
	}
	return score, gpuId
}

func SubGpuMilli(gpu types.GPU, job types.Job) {
	gpu.(*simulator.GPU).MilliGpu -= job.MilliGpu()
}

func GpuFragAmountScore(gpu types.GPU, jobList []TargetJob) float64 {
	fragAmount := GpuFragAmount(gpu, jobList)
	return fragAmount.FragAmountSum()
}

func GpuFragAmount(gpu types.GPU, jobList []TargetJob) FragAmount {
	data := make([]float64, len(FragMap))
	fragAmount := NewFragAmount(int(gpu.ID()), data)

	for _, job := range jobList {
		freq := job.Percentage
		fragType := GetGpuFrag(gpu, job.TargetJob)
		gpuMilliLeft := gpu.MilliGpuLeft()
		if fragType == Satisfied {
			fragAmount.AddByFragType(Satisfied, freq*float64(gpuMilliLeft))
		} else {
			fragAmount.AddByFragType(fragType, freq*float64(gpuMilliLeft))
		}
	}
	return fragAmount
}

func NewFragAmount(gpuId int, data []float64) FragAmount {
	fragAmount := FragAmount{GpuId: gpuId, Data: make([]float64, len(data))}
	copy(fragAmount.Data, data)
	return fragAmount
}

func GetGpuFrag(gpu types.GPU, job types.Job) string {
	if job.MilliGpu() == 0 {
		return Satisfied
	}
	if CanGpuHostJob(gpu, job) {
		return Satisfied
	} else {
		return NotSatisfied
	}
}

func CanGpuHostJob(gpu types.GPU, job types.Job) bool {
	if gpu.MilliGpuLeft() >= job.MilliGpu() {
		return true
	}
	return false
}

func (fa FragAmount) AddByFragType(fragType string, amount float64) error {
	if amount < 0 {
		return fmt.Errorf("bad freq")
	}
	if index, ok := FragMap[fragType]; !ok {
		return fmt.Errorf("bad fragType")
	} else {
		fa.Data[index] += amount
		return nil
	}
}

func (fa FragAmount) FragAmountSum() (out float64) {
	for i := 0; i < len(FragMap); i++ {
		if i != FragMap[Satisfied] {
			out += fa.Data[i]
		}
	}
	return out
}

func sigmoid(x float64) float64 {
	return 1.0 / (1.0 + math.Exp(-x))
}
