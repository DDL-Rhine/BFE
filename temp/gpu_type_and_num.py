import pandas as pd

# 读取CSV文件
df = pd.read_csv('input.csv')

# 统计gpu_milli列中每种类型的数量
model_counts = df['gpu_milli'].value_counts().sort_index()

total_counts = df.shape[0]
ratios = model_counts / total_counts

# 创建一个新的DataFrame来存储结果
result_df = pd.DataFrame({
    'gpu_milli': model_counts.index,
    'count': model_counts.values,
    'ratio': ratios.values
})

# 将结果写入CSV文件
result_df.to_csv('popularity.csv', index=False)

print("结果已写入output.csv文件")
