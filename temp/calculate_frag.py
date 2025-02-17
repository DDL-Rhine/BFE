import pandas as pd

# 读取 CSV 文件
df = pd.read_csv('hydrapureheuristic_[0-30]_result12.csv')

# 根据 waiting_num 列进行排序
df = df.sort_values(by='waiting_num').reset_index(drop=True)

# 初始化 frag_size 列
df['frag_size'] = 0

# 遍历每一行进行 frag_size 的计算
for i, row in df.iterrows():
    if i == 0:
        # 如果是第一行，frag_size 初始化为 0
        frag_size = 0
    else:
        # frag_size 初始化为上一行的 frag_size
        frag_size = df.loc[i - 1, 'frag_size']
    
    # 计算 waiting_num=x 行的 curr_gpu_milli 和所有行的 gpu_milli 的比较结果
    curr_gpu_milli = row['curr_gpu_milli']
    
    # 过滤出具有唯一 gpu_milli 值的行（用于比较）
    unique_gpu_milli_rows = df.drop_duplicates(subset=['gpu_milli'])

    for _, compare_row in unique_gpu_milli_rows.iterrows():
        if compare_row['gpu_milli'] > curr_gpu_milli:
            # 获取 waiting_num=y 行的 ratio，并乘以 curr_gpu_milli
            added_value = compare_row['ratio'] * curr_gpu_milli
            frag_size += added_value
            #break  # 找到第一个符合条件的行后立即跳出循环

    # 检查 selected_gpu_id 是否在之前行中出现过
    for z in range(i):
        if df.loc[z, 'selected_gpu_id'] == row['selected_gpu_id']:
            # 若找到匹配的 previous_row，减去其 frag_size
            unique_gpu_milli_rows = df.drop_duplicates(subset=['gpu_milli'])
            count = 0
            for _, compare_row in unique_gpu_milli_rows.iterrows():
                if compare_row['gpu_milli'] > df.loc[z, 'curr_gpu_milli']:
                    added_value = compare_row['ratio'] * df.loc[z, 'curr_gpu_milli']
                    count += added_value
            #frag_size -= df.loc[z, 'frag_size']
            frag_size -= count
            break

    # 将计算结果存储到 frag_size 列
    df.at[i, 'frag_size'] = frag_size

# 输出计算后的 DataFrame
print(df[['selected_gpu_id', 'gpu_milli', 'curr_gpu_milli', 'ratio', 'waiting_num', 'frag_size']])

# 保存为新的 CSV 文件
df.to_csv('hydrapureheuristic_[0-30]_result12.csv', index=False)
