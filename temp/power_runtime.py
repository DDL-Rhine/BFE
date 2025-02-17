import json

# 打开并按行读取 JSON 文件
with open('new_result4.json', 'r') as file:
    lines = file.readlines()

# 初始化变量
hydra_found = False
case_range_found = False
case_range_target = '"case_range": [0,100],'  # 完整的 case_range 标志
gpu_runtime = {"A100": 0, "GTX2080Ti": 0, "V100": 0}
jobs = []  # 存储 10 个 job
job = None  # 临时存储单个 job
job_count = 0

# 遍历文件的每一行
for i, line in enumerate(lines):
    # 找到包含 "HydraPureHeuristic" 的行
    if "HydraPureHeuristic" in line:
        hydra_found = True

    # 如果找到了 "HydraPureHeuristic"，接着寻找完整的 "case_range"
    if hydra_found and not case_range_found:
        if i + 3 < len(lines):
            range_str = lines[i].strip() + lines[i + 1].strip() + lines[i + 2].strip() + lines[i + 3].strip()
            if range_str == case_range_target:
                case_range_found = True
                continue

    # 如果找到了完整的 "case_range"，接着寻找包含 job 信息的行
    if case_range_found:
        # 检查是否是 JSON 对象的开头
        if line.strip().startswith("{") and job_count < 100:
            job = {}  # 初始化一个新的 job 数据

        # 判断 JSON 对象是否在解析范围内
        elif job is not None and ":" in line:  # 确保 job 已初始化
            key_value = line.strip().rstrip(",").split(": ", 1)
            if len(key_value) == 2:  # 确保键值对结构正确
                key = key_value[0].strip('"')
                value = key_value[1].strip('"')

                # 将值转换为适当的数据类型
                if value.isdigit():
                    value = int(value)
                elif value.replace('.', '', 1).isdigit():
                    value = float(value)

                job[key] = value

        # 检查 JSON 对象是否结束
        if job is not None and (line.strip().startswith("},") or line.strip().startswith("}")):
            jobs.append(job)  # 将解析完的 job 添加到列表
            job = None  # 重置 job
            job_count += 1

        # 如果已找到 10 个 job，处理数据
        if job_count == 100:
            # 分组统计每个 selected_gpu_id 的最小 start_time 和最大 end_time
            gpu_id_times = {}
            for job in jobs:
                gpu_id = job["selected_gpu_id"]
                start_time = job["start_time"]
                end_time = job["end_time"]

                if gpu_id not in gpu_id_times:
                    gpu_id_times[gpu_id] = {"min_start_time": start_time, "max_end_time": end_time}
                else:
                    gpu_id_times[gpu_id]["min_start_time"] = min(gpu_id_times[gpu_id]["min_start_time"], start_time)
                    gpu_id_times[gpu_id]["max_end_time"] = max(gpu_id_times[gpu_id]["max_end_time"], end_time)

            # 计算每个 GPU 的总运行时间
            for gpu_id, times in gpu_id_times.items():
                runtime = times["max_end_time"] - times["min_start_time"]
                gpu_type = [job["selected_gpu_type"] for job in jobs if job["selected_gpu_id"] == gpu_id][0]
                if gpu_type in gpu_runtime:
                    gpu_runtime[gpu_type] += runtime

            # 重置状态
            hydra_found = False
            case_range_found = False
            jobs = []
            job_count = 0

# 输出结果
for gpu_type, total_runtime in gpu_runtime.items():
    print(f"Total runtime for {gpu_type}: {total_runtime}")
