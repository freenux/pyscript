import csv
import argparse
from collections import OrderedDict

def merge_csv(file1, file2, output_file, sort_field, dedup_field):
    """合并两个CSV文件，按指定字段排序并去重"""
    # 读取第一个CSV文件
    with open(file1, 'r', encoding='utf-8') as f1:
        reader = csv.DictReader(f1)
        data = list(reader)

    # 读取第二个CSV文件
    with open(file2, 'r', encoding='utf-8') as f2:
        reader = csv.DictReader(f2)
        data.extend(list(reader))

    # 按指定字段排序
    sorted_data = sorted(data, key=lambda x: x[sort_field])

    # 按去重字段进行去重（保留第一个出现的记录）
    seen = OrderedDict()
    for row in sorted_data:
        key = row[dedup_field]
        if key not in seen:
            seen[key] = row

    # 获取所有字段名（保留第一个文件的字段顺序）
    fieldnames = reader.fieldnames if reader.fieldnames else data[0].keys()

    # 写入结果文件
    with open(output_file, 'w', encoding='utf-8', newline='') as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(seen.values())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='合并CSV文件并进行排序去重')
    parser.add_argument('input1', help='第一个输入CSV文件路径')
    parser.add_argument('input2', help='第二个输入CSV文件路径')
    parser.add_argument('output', help='输出CSV文件路径')
    parser.add_argument('-s', '--sort', required=True, help='排序字段名称')
    parser.add_argument('-d', '--dedup', required=True, help='去重字段名称')
    
    args = parser.parse_args()
    
    merge_csv(args.input1, args.input2, args.output, args.sort, args.dedup)
    print(f"文件合并完成，结果已保存至: {args.output}")