package cost

import (
	"DES-go/schedulers/types"
	"DES-go/simulator"
	"DES-go/util"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
)

// solverCommon 定义了成本计算的公用类型。
type solverCommon struct {
	memo            *sync.Map // map[string]*Resp
	jctOffsetGetter jctOffsetGetter
}

func newCostSolverCommon(defaultJCTOffsetGetter jctOffsetGetter) *solverCommon {
	return &solverCommon{
		memo:            &sync.Map{},
		jctOffsetGetter: defaultJCTOffsetGetter,
	}
}

type jctOffsetGetter func(gpu types.GPU) types.Time

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

func (c *solverCommon) costMemoKey(gpu types.GPU, jobs []types.Job, jctOffset types.Time) string {
	builder := &strings.Builder{}
	// gpu info
	builder.WriteString("GPU:")
	builder.WriteString(gpu.String())
	builder.WriteString("JCTOffset:")
	builder.WriteString(strconv.FormatFloat(float64(jctOffset), 'f', 6, 64))
	writeJob := func(job types.Job) {
		builder.WriteString(string(job.JobName()))
		builder.WriteString(strconv.FormatFloat(job.RemainingRatio(), 'f', 6, 64))
		builder.WriteByte('-')
	}
	builder.WriteString("Jobs:")
	for _, job := range jobs {
		writeJob(job)
	}
	return builder.String()
}

func CalPercentage(job types.Job, waitingJobs []types.Job) float64 {
	milliGpu := job.MilliGpu()
	count := 0.
	for i := range waitingJobs {
		if waitingJobs[i].MilliGpu() == milliGpu {
			count++
		}
	}
	return float64(count / float64(len(waitingJobs)))
}
func (c *solverCommon) CalJCTAndDDLViolations(jctOffset types.Time, gpu types.GPU, jobs []types.Job, waitingJobs []types.Job, currTime types.Time) (types.Time, []types.Duration, []types.Job, int64) {
	maxNum := 0
	maxIdx := 0
	JCTs := make([]types.Time, 0, len(jobs))
	var JCT types.Time
	ddlViolations := make([]types.Duration, 0, len(jobs))
	ddlViolatedJobs := make([]types.Job, 0)
	//frags := make([]int64, 0, len(jobs))
	jobList := make([]TargetJob, len(waitingJobs)) //waitingJobs
	for l := range waitingJobs {
		percentage := CalPercentage(waitingJobs[l], waitingJobs)
		jobList[l] = TargetJob{
			TargetJob:  waitingJobs[l],
			Percentage: percentage,
		}
	}
	for idx, job := range jobs {
		if int(job.WaitingNum()) > maxNum {
			maxNum = int(job.WaitingNum())
			maxIdx = idx
		}
	}
	for idx, job := range jobs {
		// 此处是预测job的JCT，不是计算已经完成的任务的JCT，所以不可以调用job.JCT()，因为job.JCT()只有当任务实际已经完成时才能返回结果。
		//currJobJCT := jctOffset + types.Time(job.RemainingDuration(gpu.Type())) - job.JobMeta().SubmitTime()
		var currJobJCT types.Time
		//if int(job.WaitingNum()) > maxNum {
		//	maxNum = int(job.WaitingNum())
		//	maxIdx = idx
		//}
		if job.FirstExecutionTime() != -1 {
			currJobJCT = job.FirstExecutionTime() + types.Time(job.RemainingDuration(gpu.Type())) - job.JobMeta().SubmitTime()
			//currJobJCT = types.Time(job.RemainingDuration(gpu.Type())) - job.JobMeta().SubmitTime()
		} else {
			currJobJCT = currTime + types.Time(job.RemainingDuration(gpu.Type())) - job.JobMeta().SubmitTime()
			//currJobJCT = types.Time(job.RemainingDuration(gpu.Type())) - job.JobMeta().SubmitTime()
		}
		//jctOffset = currJobJCT
		JCTs = append(JCTs, currJobJCT)
		if currJobJCT > job.JobMeta().DDL() {
			ddlViolations = append(ddlViolations, types.Duration(currJobJCT-job.JobMeta().DDL()))
			ddlViolatedJobs = append(ddlViolatedJobs, job)
		} else {
			ddlViolations = append(ddlViolations, 0)
		}
		if idx == maxIdx {
			JCT = currJobJCT
		}
		//frag, _ := CalculateFrag(gpu, job, jobList)
		//frags = append(frags, frag)
	}
	frag, _ := CalculateFrag(gpu, jobs[maxIdx], jobList)
	//frags = append(frags, frag)
	return JCT, ddlViolations, ddlViolatedJobs, frag
}

func (c *solverCommon) JCTOffset(gpu types.GPU) types.Time {
	return c.jctOffsetGetter(gpu)
}

// Solver 定义成本计算方式。
type Solver interface {
	Cost(gpu types.GPU, jobs []types.Job, waitingJobs []types.Job, currTime types.Time) *Resp
	JCTOffset(gpu types.GPU) types.Time
}

type SolverMaker func(jctOffsetGetter jctOffsetGetter) Solver

type DDLCostType int

const (
	DDLCostTypeStrict DDLCostType = 0 // Strict，表示严格的DDL要求，即只要违约了DDL一点点，就认为非常严重。
	DDLCostTypeSoft   DDLCostType = 1 // Soft，表示较为宽松的DDL要求。
)

type Resp struct {
	Cost            float64
	JCTCost         float64
	DDLCost         float64
	DDLViolatedJobs []types.Job
	DDLViolated     bool
	FragCost        float64
}

// ---------------------------------------- SimpleAddSolver ---------------------------------------

// SimpleAddSolver 简单的将JCT与DDL violation相加作为cost。参数可以指定ddl系数等。
type SimpleAddSolver struct {
	*solverCommon
	ddlCostType              DDLCostType
	ddlStrictCostCoefficient float64
}

func NewSimpleAddCostSolverMaker(ddlCostType DDLCostType, ddlStrictCostCoefficient float64) SolverMaker {
	return func(defaultJCTOffsetGetter jctOffsetGetter) Solver {
		return &SimpleAddSolver{
			solverCommon:             newCostSolverCommon(defaultJCTOffsetGetter),
			ddlCostType:              ddlCostType,
			ddlStrictCostCoefficient: ddlStrictCostCoefficient,
		}
	}
}

// Cost 本函数为该算法的核心部分，它定义在一个gpu上的一组排序号的jobs，它的总代价是多大。
// 目前将代价分为两部分，一部分是JCT，另一部分是DDL违约。
// 那么JCT就按照累加求和即可，而DDL作为更为首要的要求，可以使用一个高倍的系数，乘以每个违约job的违约时长，使得它比JCT更重要。
// 那么这里也可以加入soft DDL的模式，即当job只违反了一点点DDL时，不认为它非常严重。
// 返回值: 分别返回，代价的大小（float64），以及是否存在DDL违反（bool）。
func (s *SimpleAddSolver) Cost(gpu types.GPU, jobs []types.Job, waitingJobs []types.Job, currTime types.Time) *Resp {
	// 如果有过相同调用，则直接返回记录的结果。
	jctOffset := s.jctOffsetGetter(gpu)
	memoKey := s.costMemoKey(gpu, jobs, jctOffset)
	if memorized, ok := s.memo.Load(memoKey); ok {
		return memorized.(*Resp)
	}

	// 第一步，计算每个任务的jct，以及每个任务违反ddl的时长。
	JCT, ddlViolations, ddlViolatedJobs, frag := s.CalJCTAndDDLViolations(jctOffset, gpu, jobs, waitingJobs, currTime)

	// 第二步，计算jct带来的cost。
	JCTCost := func() float64 {
		// 目前，简单的将JCT求和后的值作为JCT Costs。这里在后面可以进行修改，比如增加一些系数。
		//interfaceJCTs := make([]interface{}, len(JCTs))
		//for idx, jct := range JCTs {
		//	interfaceJCTs[idx] = jct
		//}
		//return util.SumFloat64(func(item interface{}) float64 {
		//	return float64(item.(types.Time))
		//}, interfaceJCTs...)
		return float64(JCT)
	}()

	// 第三步，计算DDL violation带来的cost。
	DDLCost := func() float64 {
		// 计算每个job的ddl violation Cost，并求和
		ddlViolationCosts := make([]interface{}, 0, len(ddlViolations))
		for _, violation := range ddlViolations {
			if violation <= 0 {
				continue
			}
			switch s.ddlCostType {
			case DDLCostTypeStrict:
				ddlViolationCosts = append(ddlViolationCosts, s.ddlStrictCostCoefficient*float64(violation))
			case DDLCostTypeSoft:
				// TODO
			default:
				panic("Unsupported ddlCostType")
			}
		}
		return util.SumFloat64(func(item interface{}) float64 {
			return item.(float64)
		}, ddlViolationCosts...)
	}()

	FragCost := func() int64 {
		//GPUFragCost := make([]interface{}, len(frags))
		//for idx, GPUFrag := range frags {
		//	GPUFragCost[idx] = GPUFrag
		//}
		//return util.SumInt64(func(item interface{}) int64 {
		//	return item.(int64)
		//}, GPUFragCost...)
		return frag
	}()

	// 第四步，求和即可。这里实际上还是可以继续调参，比如对他们分别乘以系数。
	costResp := &Resp{
		Cost: JCTCost + DDLCost + float64(FragCost),
		//Cost:            float64(FragCost),
		JCTCost:         JCTCost,
		DDLCost:         DDLCost,
		DDLViolated:     DDLCost > 0,
		DDLViolatedJobs: ddlViolatedJobs,
		FragCost:        float64(FragCost),
	}
	s.memo.Store(memoKey, costResp)
	return costResp
}

type MinCostParams struct {
	CostSolver  Solver
	GPU         types.GPU
	Jobs        []types.Job
	WaitingJobs []types.Job
	CurrTime    types.Time
}

type MinCostAlgo interface {
	MinCost(params *MinCostParams) (float64, []types.Job)
	String() string
	RecordExtra() interface{}
}

func CalculateFrag(gpu types.GPU, job types.Job, jobList []TargetJob) (score int64, gpuId int64) {
	//gpuFragScore := GpuFragAmountScore(gpu, jobList)
	score, gpuId = 0, -1
	if gpu.MilliGpuLeft() >= job.MilliGpu() {
		//newGpu := gpu.Copy()
		gpu.(*simulator.GPU).MilliGpu -= job.MilliGpu()
		//**********************
		job.SetSelectedGpu(gpu)
		//**********************
		newGpuFragScore := GpuFragAmountScore(gpu, jobList, job)
		gpu.(*simulator.GPU).MilliGpu += job.MilliGpu()
		//*************************
		job.SetSelectedGpu(nil)
		//*************************
		//fragScore := int64(sigmoid((gpuFragScore-newGpuFragScore)/1000) * 100)
		fragScore := int64(newGpuFragScore)
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

func GpuFragAmountScore(gpu types.GPU, jobList []TargetJob, job types.Job) float64 {
	fragAmount := GpuFragAmount(gpu, jobList, job)
	//return fragAmount.FragAmountSum()
	return fragAmount
}

func GpuFragAmount(gpu types.GPU, jobList []TargetJob, job1 types.Job) float64 { //FragAmount
	data := make([]float64, len(FragMap))
	fragAmount := NewFragAmount(int(gpu.ID()), data)
	//fragSize := 0.

	uniqueJobs := make(map[float64]TargetJob) // 创建一个映射存储唯一的job.Percentage
	for _, job := range jobList {
		if _, exists := uniqueJobs[job.Percentage]; !exists {
			uniqueJobs[job.Percentage] = job // 仅添加唯一的 Percentage
		}
	}
	//for idx, currJob := range jobList {
	//	if idx == 0 {
	//		currJob.TargetJob.SetFragAmount(0)
	//	} else {
	//		currJob.TargetJob.SetFragAmount(jobList[idx-1].TargetJob.FragAmount())
	//	}
	//uniqueJobs := make(map[float64]TargetJob) // 创建一个映射存储唯一的job.Percentage
	//for _, job := range jobList {
	//	if _, exists := uniqueJobs[job.Percentage]; !exists {
	//		uniqueJobs[job.Percentage] = job // 仅添加唯一的 Percentage
	//	}
	//}
	//************************************************
	//for _, job := range uniqueJobs {
	//	freq := job.Percentage
	//	fragType := GetGpuFrag(gpu, job.TargetJob)
	//	gpuMilliLeft := gpu.MilliGpuLeft()
	//	if fragType == Satisfied {
	//		fragAmount.AddByFragType(Satisfied, freq*float64(gpuMilliLeft))
	//	} else {
	//		fragAmount.AddByFragType(fragType, freq*float64(gpuMilliLeft))
	//		//currJob.TargetJob.SetFragAmount(currJob.TargetJob.FragAmount() + freq*float64(gpuMilliLeft))
	//	}
	//}
	//************************************************
	//for z := 0; z < idx; z++ {
	//	if jobList[z].TargetJob.SelectedGpu() == gpu {
	//		count := 0.
	//		for _, job := range uniqueJobs {
	//			freq := job.Percentage
	//			fragType := GetGpuFrag(jobList[z].TargetJob.SelectedGpu(), job.TargetJob)
	//			gpuMilliLeft := jobList[z].TargetJob.SelectedGpu().MilliGpuLeft()
	//			if fragType != Satisfied {
	//				//fragAmount.AddByFragType(fragType, freq*float64(gpuMilliLeft))
	//				//currJob.TargetJob.SetFragAmount(fragSize + freq*float64(gpuMilliLeft))
	//				count += freq * float64(gpuMilliLeft)
	//			}
	//		}
	//		currJob.TargetJob.SetFragAmount(currJob.TargetJob.FragAmount() - count)
	//		break
	//	}
	//}
	//if currJob.TargetJob == job1 {
	//	return currJob.TargetJob.FragAmount()
	//}
	//}

	var milliGpusLeft int64 = 45000
	var tempSum int64 = 0
	gpus := make([]types.GPU, 0, len(jobList))
	for _, job := range jobList {
		//gpus = append(gpus, job.TargetJob.SelectedGpu())
		//if int64(idx) == job1.WaitingNum() {
		//	break
		//}
		if job.TargetJob.SelectedGpu() != nil {
			gpus = append(gpus, job.TargetJob.SelectedGpu())
			tempSum += job.TargetJob.MilliGpu()
		}
	}
	milliGpusLeft -= tempSum
	uniqueGpus := make(map[types.GPUID]types.GPU)
	for _, gpu1 := range gpus {
		if _, exists := uniqueGpus[gpu1.ID()]; !exists {
			uniqueGpus[gpu1.ID()] = gpu1
		}
	}
	sum := 0.
	for _, gpu1 := range uniqueGpus {
		for _, job := range uniqueJobs {
			freq := job.Percentage
			fragType := GetGpuFrag(gpu1, job.TargetJob)
			gpuMilliLeft := gpu1.MilliGpuLeft()
			if fragType == Satisfied {
				fragAmount.AddByFragType(Satisfied, freq*float64(gpuMilliLeft))
			} else {
				fragAmount.AddByFragType(fragType, freq*float64(gpuMilliLeft))
				//currJob.TargetJob.SetFragAmount(currJob.TargetJob.FragAmount() + freq*float64(gpuMilliLeft))
				//sum += float64(1000) * freq * float64(gpuMilliLeft) / float64(job.TargetJob.MilliGpu())
				sum += freq * float64(gpuMilliLeft)
			}
		}
	}
	//return fragAmount
	result := sum * 10
	//return float64(100) * sum / float64(milliGpusLeft)
	return result
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
