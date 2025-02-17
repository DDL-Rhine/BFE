# BFE
BFE is a fragmentation-aware and efficiency-oriented scheduler for tasks on heterogeneous GPU clusters. It significantly improve the GPU utilization while minimizing the task completion time (TCT) by optimizing our novel twofold scheduling objective.

This repository contains the scheduler implementation and a heterogeneous GPU cluster simulator. The Kubernetes ported version is in another developing [repository](https://github.com/MLSched/UNS).

## Highlights
- **Fragmentation measure: **We propose a GPU fragmentation measurement to assess the degree of GPU fragmentation in the cluster, considering the task size and the GPU heterogeneity.
- **Math Computation of GPU Heterogeneity: **We provide a scientific mathematical computation method when considering GPU heterogeneity that improves GPU utilization and task completion time in heterogeneous environment.
- **Novel Scheduling Algorithm: **We present a task scheduling algorithm based on Particle Swarm Optimization to minimize the cluster's average TCT and GPU fragmentation size.

## Contents
```
.
├── cases: all test cases.
├── data: output metrics data.
├── metrics: code for metrics.
├── schedulers: schedulers implementations.
├── simulator: heterogenous GPU cluster simulator.
├── config.json: configuration file.
├── main.go: entry point.
└── util: helper codes.

```
## Requirements
Linux OS (e.g., CentOS, RedHat, Ubuntu).

Python == 3.9

Go == 1.17.9

## How to prepare the data
```
cd cases
python3 pod_norm.py
```
## How to use
```
cd ..
go run main.go
```