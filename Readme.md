# DES-go: Hydra Scheduler Go Implementation

BFE is a discrete event-driven heterogeneous GPU cluster scheduler implementation based on Go language. This project focuses on task completion time and GPU fragmentation optimization for deep learning training jobs, providing efficient resource allocation and job scheduling capabilities in heterogeneous GPU cluster environments.

## Project Overview

This project is the Go language version of the BFE scheduler, which includes an event-driven heterogeneous GPU cluster simulator and implementations of multiple scheduling algorithms. The project adopts a modular design and supports various scheduling strategies and performance evaluation metrics.

## Project Structure

```
.
├── cases/           # Test cases and datasets
├── data/           # Output performance metric data
├── metrics/        # Performance metric calculation code
├── schedulers/     # Scheduler algorithm implementations
├── simulator/      # Heterogeneous GPU cluster simulator
├── util/           # Utility functions and helper code
├── config.json     # Configuration file
├── main.go         # Program entry point
├── go.mod          # Go module dependencies
├── go.sum          # Go module checksums
└── Dockerfile      # Containerized deployment file
```

## System Requirements

- Linux operating system (e.g., CentOS, RedHat, Ubuntu)
- Go 1.17 or higher
- Optional: Docker >= 20.10.13 (for containerized deployment)

## Installation and Execution

### Local Execution

1. **Clone the project**
   ```bash
   cd /path/to/BFE
   ```

2. **Install Go dependencies**
   ```bash
   go mod tidy
   ```

3. **Configure parameters (optional)**
   Edit the `config.json` file to customize simulation parameters:
   ```json
   {
     "cases_path": "/home/lab/temp/BFE/cases/",
     "reports_path": "/home/lab/temp/BFE/reports/",
     "workload": "light",
     "number_of_jobs": [0, 200]
   }
   ```

4. **Run the scheduler**
   ```bash
   # Run with default configuration
   go run main.go
   
   # Or specify configuration file path
   go run main.go /path/to/config.json
   ```

## Configuration Description

### Configuration File Parameters

- **cases_path**: Specify the dataset path
- **reports_path**: Specify the experimental results output path
- **workload**: Workload type, options are "light" or "heavy"
- **number_of_jobs**: Job quantity range, containing two integers forming a range
- **algorithms**: List of algorithms to test
  - `BFE`: Hydra algorithm
  - `allox`, `gavel`, `chronus`: Other comparison scheduling algorithms

### Cluster Configuration

The project supports heterogeneous clusters with the following GPU types:
- **V100**
- **GTX2080Ti**
- **A100**

## Algorithm Implementation

### Hydra Scheduler
- **Time-aware**: Considers task completion time (TCT) in the cluster
- **GPU fragmentation-aware**: Considers GPU fragmentation generated in the cluster for scheduling decisions
- **Heterogeneity-aware**: Optimizes resource allocation for different GPU types

### Comparison Algorithms
- **Allox**: Resource allocation algorithm
- **Gavel**: Heterogeneity-aware scheduling algorithm
- **Chronus**: Time-aware scheduling algorithm

## Performance Metrics

The simulator outputs the following key performance metrics:

- **Average Job Completion Time (JCT)**: Average time from job submission to completion
- **Average Queue Delay**: Average time jobs wait in the queue
- **GPU Resource Quantity**: Number of GPU resources currently used by tasks in the cluster (0-1000). For convenient representation of GPU resource values, GPU memory is divided into 1000 parts
- **Scheduler Execution Time**: Average execution time for scheduling decisions

## Results Analysis

Experimental results are saved in JSON format in the `reports/` directory with filename format `[name]-[datetime].json`. Results include:

- Simulation meta-parameters (case name, job range, cluster configuration)
- Detailed performance metrics for each scheduler
- Additional execution records for specific schedulers

## Datasets

### Pre-generated Datasets
- `cases/norm_openb_pod_list_gpushare20_new.csv`: Workload dataset

### Custom Datasets
Custom test cases can be generated using Alibaba cluster trace data.

## Notes

- When the number of jobs is set to a large value (300~400), evaluation speed will be slow
- It is recommended to test with light workloads first
- Scheduler performance may vary depending on cluster configuration and workload characteristics

## Contributing

Issues and Pull Requests are welcome to improve the project.

## License

Please refer to the LICENSE file in the project root directory.

## Related Projects

- [Original Hydra Project](https://github.com/MLSched/UNS) - Kubernetes ported version
- [Alibaba Cluster Data](https://github.com/alibaba/clusterdata) - Data source for generating test cases
