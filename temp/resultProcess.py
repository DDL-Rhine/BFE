import json
import csv
import pandas as pd

df = pd.read_csv("popularity.csv")
key_value_pairs = df.set_index(df.columns[0]).to_dict()[df.columns[2]]

# 读取文件1
with open('no_weight_result.json', 'r') as f1:
    data1 = json.load(f1)

# 读取文件2
file2_data = {}
with open('new_input1.csv', 'r') as f2:
    reader = csv.DictReader(f2)
    for row in reader:
        file2_data[row['job_name']] = row

# 生成CSV文件
for algorithm, reports in data1['reports'].items():
    for report in reports:
        case_range = report['case_range']
        case_range_str = f"[{case_range[0]}-{case_range[1]}]"
        filename = f"{algorithm.lower()}_{case_range_str}_no_weight_result.csv"

        with open(filename, 'w', newline='') as csvfile:
            #fieldnames = ['job_name', 'selected_gpu_id', 'selected_gpu_type', 'runtime_on_gpu', 'gpu_milli', 'A100', 'GTX2080Ti', 'V100', 'ddl']
            fieldnames = ['job_name', 'selected_gpu_id', 'runtime_on_gpu', 'gpu_milli', 'curr_gpu_milli', 'ratio', 'A100', 'GTX2080Ti', 'V100', 'ddl', 'waiting_num']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            if report['job_reports'] is not None:
                for job_report in report['job_reports']:
                    job_name = job_report['job_name']
                    if job_name in file2_data:
                        ratio = 0.0
                        count = 0.0
                        gpu_milli = file2_data[job_name]['gpu_milli']
                        #for key, value in key_value_pairs.items():
                        #    key = int(key)
                        #    gpu_milli = int(gpu_milli)
                        #    if key == gpu_milli:
                        #        ratio = value
                        #        break
                        for job_report1 in report['job_reports']:
                            job_name1 = job_report1['job_name']
                            if gpu_milli == file2_data[job_name1]['gpu_milli']:
                                count = count + 1
                        ratio = count / len(report['job_reports'])
                        row = {
                            'job_name': job_report['job_name'],
                            'selected_gpu_id': job_report['selected_gpu_id'],
                            #'selected_gpu_type': job_report['selected_gpu_type'],
                            'runtime_on_gpu': job_report['runtime_on_gpu'],
                            'gpu_milli': file2_data[job_name]['gpu_milli'],
                            'curr_gpu_milli': job_report['curr_gpu_milli'],
                            'ratio': ratio,
                            'A100': file2_data[job_name]['A100'],
                            'GTX2080Ti': file2_data[job_name]['GTX2080Ti'],
                            'V100': file2_data[job_name]['V100'],
                            'ddl': file2_data[job_name]['ddl'],
                            'waiting_num': job_report['waiting_num']
                        }
                        writer.writerow(row)
            else:continue
