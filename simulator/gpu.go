package simulator

import (
	"DES-go/schedulers/types"
	"fmt"
)

//type GPUType string
//type GPUID int

type GPU struct {
	gpuID      types.GPUID
	gpuType    types.GPUType
	MilliGpu   int64
	fragAmount float64
}

func NewGPU(gpuID types.GPUID, gpuType types.GPUType) *GPU {
	return &GPU{
		gpuID:    gpuID,
		gpuType:  gpuType,
		MilliGpu: 1000,
	}
}

func (g *GPU) ID() types.GPUID {
	return g.gpuID
}

func (g *GPU) Type() types.GPUType {
	return g.gpuType
}

func (g *GPU) String() string {
	return fmt.Sprintf("gpu=[ID=%d, Type=%s]", g.ID(), g.Type())
}

func (g *GPU) MilliGpuLeft() int64 {
	return g.MilliGpu
}

func (g *GPU) FragAmount() float64 {
	return g.fragAmount
}

func (g *GPU) SetFragAmount(fragAmount float64) {
	g.fragAmount = fragAmount
}

/*func (g *GPU) Copy() types.GPU {
	return types.GPU{
		gpuID:    g.gpuID,
		gpuType:  g.gpuType,
		MilliGpu: g.MilliGpu,
	}
}*/
