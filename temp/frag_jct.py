import pandas as pd

# 读取文件1
#file1 = pd.read_csv('gavel_[0-20]_result.csv')
file1 = pd.read_csv('hydrapureheuristic_[0-100]_new_result1.csv')

# 读取文件2
file2 = pd.read_csv('popularity.csv')

# 计算frag_size和frag_size_runtime
#def calculate_frag_size(gpu_milli, runtime_on_gpu, file2):
#    frag_size = (1000 - gpu_milli) * file2[file2['gpu_milli'] < (1000-gpu_milli)]['ratio'].sum()
#    frag_size_runtime = frag_size * runtime_on_gpu
#    return frag_size, frag_size_runtime

def calculate_frag_size(curr_gpu_milli, runtime_on_gpu, file1):
    frag_size = curr_gpu_milli * file1[file1['gpu_milli'] > curr_gpu_milli]['ratio'].sum()
    frag_size_runtime = frag_size * runtime_on_gpu
    return frag_size, frag_size_runtime

# 为文件1添加新列
file1['frag_size'] = file1.apply(lambda row: calculate_frag_size(row['curr_gpu_milli'], row['runtime_on_gpu'], file1)[0], axis=1)
file1['frag_size_runtime'] = file1.apply(lambda row: calculate_frag_size(row['curr_gpu_milli'], row['runtime_on_gpu'], file1)[1], axis=1)

# 保存结果到新的CSV文件
#file1.to_csv('frag_gavel_[0-20]_result.csv', index=False)
file1.to_csv('hydrapureheuristic_[0-100]_new_result1.csv', index=False)

print("碎片化计算完成")
