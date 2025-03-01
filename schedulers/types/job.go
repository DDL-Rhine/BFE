package types

type JobName string

type JobExecutionDetail interface {
	SumRuntimeOnGPUs() Duration
	ExecutionRanges() map[GPU][]JobExecutionRange
}

type JobExecutionRange interface {
	TimeRange() TimeRange
}

type TimeRange interface {
	Start() Time
	End() Time
	Runtime() Duration
}

type Job interface {
	JobName() JobName
	ExecutionDetail() JobExecutionDetail
	FirstExecutionTime() Time
	FinishExecutionTime() Time
	RemainingRatio() float64
	RemainingDuration(gpuType GPUType) Duration
	IsRunning() bool
	IsFinished() bool
	QueueDelay() Duration
	JobMeta() JobMeta
	Violation() (bool, Duration)
	JCT() Time
	HasDDL() bool
	ActualRuntimeOnGPUs() Duration
	MilliGpu() int64
	CurrGPUMilli() int64
	SetCurrGPUMilli(currGPUMilli int64)
	WaitingNum() int64
	SetWaitingNum(waitingNum int64)
	SelectedGpu() GPU
	SetSelectedGpu(selectedGpu GPU)
	FragAmount() float64
	SetFragAmount(fragAmount float64)
}

type JobMeta interface {
	JobName() JobName
	DDL() Time
	Durations() map[GPUType]Duration
	Duration(gpu GPU) Duration
	SubmitTime() Time
	MilliGpu() int64
}
