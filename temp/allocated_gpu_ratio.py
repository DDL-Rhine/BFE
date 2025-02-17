import pandas as pd

# 读取 CSV 文件
df = pd.read_csv('hydrapureheuristic_[0-100]_new_no_penalty_result_1.csv')  # 请替换为你的文件名

df = df.sort_values(by='waiting_num').reset_index(drop=True)

# 按 selected_gpu_id 分组
grouped = df.groupby('selected_gpu_id')

# 初始化 sum
#sum_value = grouped['gpu_milli'].first().sum()
sum_value = 0
#print(sum_value)

#total_count = df['gpu_milli'].sum()
#print(total_count)

# 初始化上一个和
#previous_sum = None

# 遍历每个分组
for gpu_id, group in grouped:
    # 计算 gpu_milli 和 curr_gpu_milli 的和
    group['total_milli'] = group['gpu_milli'] + group['curr_gpu_milli']

    previous_total = None

    for indx, row in group.iterrows():
        current_total = row['total_milli']

        if previous_total is not None and current_total >= previous_total:
            # 如果不递减，停止检查该组
            break

        # 如果递减，将gpu_milli加入sum_value
        sum_value += row['gpu_milli']

        # 更新previous_total
        previous_total = current_total

# 输出结果
print(f"Sum of gpu_milli: {sum_value}")
