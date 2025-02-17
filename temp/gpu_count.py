import pandas as pd

# 读取CSV文件
df = pd.read_csv('hydrapureheuristic_[0-100]_new_result4.csv')

# 统计gpu_milli列中每种类型的数量
model_counts = df['selected_gpu_id'].value_counts().sort_index()

# 按照selected_gpu_id进行分组
grouped = df.groupby('selected_gpu_id')

# 统计每个分组的数量
group_counts = grouped.size()

# 统计大于等于0且小于等于19、大于等于20且小于等于34、大于等于35且小于等于44的组数
group_count_0_19 = group_counts[(group_counts.index >= 0) & (group_counts.index <= 19)].count()
group_count_20_34 = group_counts[(group_counts.index >= 20) & (group_counts.index <= 34)].count()
group_count_35_44 = group_counts[(group_counts.index >= 35) & (group_counts.index <= 44)].count()

print("使用的GPU个数:", len(model_counts.values))
print("A100个数:", group_count_0_19)
print("2080Ti个数:", group_count_20_34)
print("V100个数:", group_count_35_44)
