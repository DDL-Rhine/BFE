package types

type GPUType string
type GPUID int

type GPU interface {
	ID() GPUID
	Type() GPUType
	String() string
	MilliGpuLeft() int64
	FragAmount() float64
	SetFragAmount(fragAmount float64)
	//Copy() GPU
}
