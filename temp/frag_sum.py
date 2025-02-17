import pandas as pd

# 读取CSV文件
df = pd.read_csv('hydrapureheuristic_[0-100]_new_result.csv')

total_count = df.shape[0]

# 计算frag_size列的总和
frag_size_sum = df['frag_size'].sum()

# 计算frag_size_runtime列的总和
#frag_size_runtime_sum = df['frag_size_runtime'].sum()

frag_size_average = frag_size_sum / total_count

#frag_size_runtime_average = frag_size_runtime_sum / total_count

# 输出结果
print(f"frag_hydrababwithheuristic_[0-100]_result的frag_size列的总和: {frag_size_sum}")
#print(f"frag_hydrababwithheuristic_[0-100]_result的frag_size_runtime列的总和: {frag_size_runtime_sum}")
print(f"frag_hydrababwithheuristic_[0-100]_result的frag_size列的平均值: {frag_size_average}")
#print(f"frag_hydrababwithheuristic_[0-100]_result的frag_size_runtime列的平均值: {frag_size_runtime_average}")

# # 将结果写入新的CSV文件
# result_df = pd.DataFrame({
#     'metric': ['frag_size_sum', 'frag_size_runtime_sum'],
#     'value': [frag_size_sum, frag_size_runtime_sum]
# })
#
# result_df.to_csv('output_summary.csv', index=False)
#
# print("结果已写入output_summary.csv文件")
