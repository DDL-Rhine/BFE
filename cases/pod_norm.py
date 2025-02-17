import pandas as pd
import numpy as np
import random

def base_process(file_path):
    # 读取CSV文件
    df = pd.read_csv(file_path)

    # 只保留num_gpu为1的行
    df = df[df['num_gpu'] == 1]

    # 删除执行失败的任务
    #df = df[df['pod_phase'] == 'Succeeded']

    # 修改列名
    df.rename(columns={'name': 'job_name'}, inplace=True)
    # 删除重复的任务
    df.drop_duplicates(subset=['job_name'], keep='first', inplace=True)

    # 删除不需要的列
    #df.drop(columns=['cpu_milli', 'memory_mib', 'num_gpu', 'gpu_milli', 'gpu_spec', 'qos'], inplace=True)
    df.drop(columns=['cpu_milli', 'memory_mib', 'num_gpu', 'gpu_spec', 'qos'], inplace=True)

    #df.fillna(0, inplace=True)
    # 计算jct列
    df['runtime'] = df['deletion_time'] - df['scheduled_time']
    # df['jct'] = df['jct'].dt.total_seconds()

    # 过滤掉太短的任务，它们有可能是inference任务。 返回筛选和处理后的数据帧df_target.
    #min_run_time = 1000
    #df = df[df['runtime'] > min_run_time]

    df = df[df['scheduled_time'].notna()]

    # 筛选 runtime 小于 1000 和大于 1000 的行
    runtime_less_1000 = df[df['runtime'] < 1000]
    runtime_greater_1000 = df[df['runtime'] > 1000]

    # 如果 runtime < 1000 的行数不足 300，则保留所有行
    num_samples = min(len(runtime_less_1000), 300)

    # 从 runtime < 1000 的行中随机抽取 300 行
    sampled_runtime_less_1000 = runtime_less_1000.sample(n=num_samples, random_state=42)

    # 合并 runtime > 1000 和随机抽样的 runtime < 1000
    df = pd.concat([sampled_runtime_less_1000, runtime_greater_1000])

    max_gpu_milli = 1000
    df = df[df['gpu_milli'] < max_gpu_milli]

    # 按creation_time列排序
    df.sort_values(by='creation_time', inplace=True)

    # 获取最小的creation_time值
    min_creation_time = df['creation_time'].min()

    # 更新creation_time列的值
    df['creation_time'] = df['creation_time'] - min_creation_time

    # 删除deletion_time和scheduled_time列
    df.drop(columns=['deletion_time', 'scheduled_time'], inplace=True)

    # 修改列名
    df.rename(columns={'creation_time': 'norm_job_submit_time'}, inplace=True)

    # 删除pod_phase列
    df.drop(columns=['pod_phase'], inplace=True)

    df['gpu_type'] = 'A100'

    # 添加索引列
    df.reset_index(drop=True, inplace=True)
    df.index.name = ''
    df.reset_index(inplace=True)

    # 保存处理后的CSV文件
    df.to_csv('norm_openb_pod_list_gpushare20_new.csv', index=False)

    print("CSV文件处理完成！")
    return df

def gen_ddl_and_gpu_runtimes(df_one_inst, output_path):
    # 手动设置gpu_types
    gpu_types = ['A100', 'GTX2080Ti', 'V100']

    # 不同GPU的运行时间比例，将任务的标准运行时间转换为特定GPU上的运行时间。
    runtimes = {
        'A100': (1, 1),
        'GTX2080Ti': (1.4, 2.),
        'V100': (2.4, 2.66),
    }

    ddl_ratio = 10 # 截止时间比例
    ddl_range = (1.2, 3.0) # 截止时间范围
    submit_together = True # 任务是否可以一起提交
    
    # 用于生成一个任务从from_gpu类型的GPU转到to_gpu类型的GPU时的运行时间。
    def gen_runtime(from_gpu, to_gpu, origin_runtime):
        if from_gpu == to_gpu:
            return origin_runtime
        if from_gpu not in gpu_types:
            print("not in gpu_types:", from_gpu)
        to_rand = random.uniform(*runtimes[to_gpu])
        from_rand = random.uniform(*runtimes[from_gpu])
        return int(origin_runtime * to_rand / from_rand)
    
    # 用于生成一个基于任务提交时间、运行时间和随机因子的截止时间。
    def gen_ddl(norm_submit_time, runtime):
        if random.randint(0, 100) < ddl_ratio:
            return int(norm_submit_time + runtime * random.uniform(*ddl_range))
        return np.inf
    
    for gpu_type in gpu_types:
        df_one_inst.loc[:, gpu_type] = df_one_inst.apply(lambda x: gen_runtime(x.gpu_type, gpu_type, x.runtime), axis=1)
    
    df_one_inst.loc[:, 'ddl'] = df_one_inst.apply(lambda x: gen_ddl(x.norm_job_submit_time, x.runtime), axis=1)
    
    if submit_together:
        df_one_inst = df_one_inst.iloc[np.random.permutation(len(df_one_inst))]
        df_one_inst.loc[:, 'ddl'] = df_one_inst.apply(lambda x: x['ddl'] - x['norm_job_submit_time'], axis=1)
        df_one_inst.loc[:, 'norm_job_submit_time'] = df_one_inst.apply(lambda x: 0, axis=1)
    


    df_output = df_one_inst[['job_name', 'norm_job_submit_time', 'ddl', 'A100', 'GTX2080Ti', 'V100', 'gpu_milli']]

    # 添加索引列
    df_output.reset_index(drop=True, inplace=True)
    df_output.index.name = ''
    df_output.reset_index(inplace=True)

    df_output.to_csv(output_path, index=False)

def main():
    dfj=base_process('openb_pod_list_gpushare20.csv')
    dft=gen_ddl_and_gpu_runtimes(dfj, 'norm_openb_pod_list_gpushare20_new.csv')


# 调用 main 函数
if __name__ == "__main__":
    main()