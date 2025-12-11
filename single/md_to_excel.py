#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Markdown表格转Excel工具
将Markdown格式的表格转换为Excel文件，每个表格放在不同的sheet中
"""

import re
import pandas as pd
from typing import List, Tuple, Dict
import argparse
import sys
from pathlib import Path
from openpyxl.styles import Font


class MarkdownTableParser:
    """Markdown表格解析器"""
    
    def __init__(self):
        self.tables = []
    
    def parse_markdown_content(self, content: str) -> List[Dict]:
        """解析Markdown内容，提取所有表格"""
        lines = content.strip().split('\n')
        tables = []
        current_table = None
        current_title = ""
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # 检查是否是标题行
            if line.startswith('###'):
                current_title = line.replace('###', '').strip()
                i += 1
                continue
            
            # 检查是否是表格的开始（以|开头的行）
            if line.startswith('|') and '|' in line:
                # 找到表格的所有行
                table_lines = []
                j = i
                
                while j < len(lines) and lines[j].strip().startswith('|'):
                    table_line = lines[j].strip()
                    if table_line:
                        table_lines.append(table_line)
                    j += 1
                
                if len(table_lines) >= 2:  # 至少需要表头和分隔符行
                    # 解析表格
                    table_data = self._parse_table_lines(table_lines)
                    if table_data:
                        tables.append({
                            'title': current_title or f'表格{len(tables)+1}',
                            'data': table_data
                        })
                        current_title = ""  # 重置标题
                
                i = j
            else:
                i += 1
        
        return tables
    
    def _parse_table_lines(self, lines: List[str]) -> List[List[Dict]]:
        """解析表格行"""
        if len(lines) < 2:
            return []
        
        # 解析表头
        header = self._parse_table_row(lines[0])
        if not header:
            return []
        
        # 跳过分隔符行（通常是第二行，包含:---之类的）
        data_rows = []
        for line in lines[2:]:  # 从第三行开始是数据行
            if line.strip():  # 跳过空行
                row = self._parse_table_row(line)
                if row:
                    # 确保行的列数与表头一致
                    while len(row) < len(header):
                        row.append({'text': '', 'bold': False})
                    if len(row) > len(header):
                        row = row[:len(header)]
                    data_rows.append(row)
        
        # 组合表头和数据
        result = [header]
        result.extend(data_rows)
        return result
    
    def _parse_table_row(self, line: str) -> List[Dict]:
        """解析单行表格数据，返回包含文本和格式信息的字典列表"""
        # 移除首尾的|并分割
        line = line.strip()
        if line.startswith('|'):
            line = line[1:]
        if line.endswith('|'):
            line = line[:-1]
        
        # 分割每个单元格
        raw_cells = [cell.strip() for cell in line.split('|')]
        cells = []
        
        for cell in raw_cells:
            cell_data = self._parse_cell_formatting(cell)
            cells.append(cell_data)
        
        return cells
    
    def _parse_cell_formatting(self, cell_text: str) -> Dict:
        """解析单元格的格式，处理粗体标记"""
        cell_text = cell_text.strip()
        is_bold = False
        
        # 检查是否有粗体标记
        if cell_text.startswith('**') and cell_text.endswith('**') and len(cell_text) > 4:
            is_bold = True
            cell_text = cell_text[2:-2]  # 移除 ** 标记
        elif '**' in cell_text:
            # 处理部分粗体的情况
            parts = cell_text.split('**')
            if len(parts) >= 3:
                # 简化处理：如果包含**，就认为整个单元格是粗体
                is_bold = True
                cell_text = ''.join(parts)
        
        return {
            'text': cell_text,
            'bold': is_bold
        }
    
    def tables_to_excel(self, tables: List[Dict], output_file: str):
        """将表格数据转换为Excel文件"""
        if not tables:
            print("没有找到有效的表格数据")
            return
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for i, table in enumerate(tables):
                title = table['title']
                data = table['data']
                
                if not data:
                    continue
                
                # 提取文本数据创建DataFrame
                text_data = []
                for row in data:
                    text_row = [cell['text'] if isinstance(cell, dict) else str(cell) for cell in row]
                    text_data.append(text_row)
                
                if len(text_data) > 1:
                    df = pd.DataFrame(text_data[1:], columns=text_data[0])
                else:
                    df = pd.DataFrame([text_data[0]])
                
                # 清理sheet名称
                sheet_name = self._clean_sheet_name(title, i)
                
                # 写入Excel
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 应用格式
                worksheet = writer.sheets[sheet_name]
                self._apply_formatting(worksheet, data)
                
                print(f"已创建工作表: {sheet_name}")
        
        print(f"Excel文件已保存: {output_file}")
    
    def _apply_formatting(self, worksheet, data):
        """应用格式到Excel工作表"""
        for row_idx, row in enumerate(data, start=1):  # Excel行从1开始
            for col_idx, cell in enumerate(row, start=1):  # Excel列从1开始
                if isinstance(cell, dict) and cell.get('bold', False):
                    # 应用粗体格式
                    excel_cell = worksheet.cell(row=row_idx, column=col_idx)
                    excel_cell.font = Font(bold=True)
    
    def _clean_sheet_name(self, name: str, index: int) -> str:
        """清理工作表名称"""
        # Excel工作表名称限制：不能超过31个字符，不能包含 [ ] : * ? / \
        forbidden_chars = ['[', ']', ':', '*', '?', '/', '\\']
        cleaned_name = name
        
        for char in forbidden_chars:
            cleaned_name = cleaned_name.replace(char, '_')
        
        # 限制长度
        if len(cleaned_name) > 25:  # 留一些空间给编号
            cleaned_name = cleaned_name[:25]
        
        return f"{cleaned_name}_{index+1}" if cleaned_name else f"Sheet_{index+1}"


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='将Markdown表格转换为Excel文件')
    parser.add_argument('input_file', nargs='?', help='输入的Markdown文件路径')
    parser.add_argument('-o', '--output', help='输出的Excel文件路径', default='output.xlsx')
    
    args = parser.parse_args()
    
    # 如果没有提供输入文件，使用示例数据
    if args.input_file:
        try:
            with open(args.input_file, 'r', encoding='utf-8') as f:
                content = f.read()
        except FileNotFoundError:
            print(f"错误: 找不到文件 {args.input_file}")
            sys.exit(1)
        except Exception as e:
            print(f"错误: 读取文件时出错 - {e}")
            sys.exit(1)
    else:
        # 使用提供的示例数据
        content = """### 家庭资产负债表 (截至 YYYY年MM月DD日)

| 资产 (Assets) | 金额 (元) | 负债 (Liabilities) | 金额 (元) |
| :----------- | :------- | :---------------- | :-------- |
| **流动资产** | | **短期负债** | |
| 现金及活期存款 | 2,000,000 | 信用卡应付款 | 0 |
| 货币基金 | 0 | 其他短期借款 | 0 |
| 短期理财产品 | 0 | | |
| **小计** | **2,000,000** | **小计** | **0** |
| | | | |
| **投资资产** | | **长期负债** | |
| 股票 | 0 | 房屋贷款 | 0 |
| 基金 | 0 | 汽车贷款 | 0 |
| 债券 | 0 | 其他长期借款 | 0 |
| 投资性房产(市值) | (请估算) | **小计** | **0** |
| **小计** | **(请估算)** | | |
| | | | |
| **自用资产** | | **负债合计** | **0** |
| 自住房产(市值) | (请估算) | | |
| 汽车(市值) | (请估算) | **家庭净资产** | **(资产合计 - 负债合计)** |
| **小计** | **(请估算)** | | |
| | | | |
| **资产合计** | **(流动+投资+自用)** | | |


### 家庭年度收入支出表 (YYYY年度)

| 收入项目 (Income) | 月均 (元) | 年合计 (元) | 备注 |
| :--- | :--- | :--- | :--- |
| **工资收入** | | | |
| 男主税后工资 | 83,333 | 1,000,000 | |
| 女主工资收入 | (请填写) | (请填写) | |
| **理财收入** | | | |
| 房屋租金 | 5,300 | 63,600 | |
| 存款/理财利息 | (请预估) | (请预估) | |
| **收入合计 (A)** | | **(请计算)** | |
| | | | |
| **支出项目 (Expenses)** | **月均 (元)** | **年合计 (元)** | **备注** |
| **固定支出** | | | |
| 父母赡养费 | (请填写) | (请填写) | 3位老人 |
| 子女教育(学费/兴趣班) | (请填写) | (请填写) | 9岁 & 3岁 |
| **生活支出** | | | |
| 日常饮食开销 | (请填写) | (请填写) | |
| 水电燃气通讯费 | (请填写) | (请填写) | |
| 交通/养车费 | (请填写) | (请填写) | |
| 购物/娱乐/人情 | (请填写) | (请填写) | |
| **其他支出** | | | |
| 年度保险费用 | (请填写) | (请填写) | |
| 旅游基金 | (请填写) | (请填写) | |
| 医疗备用 | (请填写) | (请填写) | |
| **支出合计 (B)** | | **(请计算)** | |
| | | | |
| **年度结余 (A - B)** | | **(请计算)** | |

### 家庭理财目标表

| 目标分类 | 目标描述 | 预计需要金额 (元) | 期望达成时间 (距今几年) | 优先级 |
| :--- | :--- | :--- | :--- | :--- |
| **短期(1-3年)** | 应对失业风险的紧急储备 | 600,000 | 立即 | **最高** |
| | 家庭年度旅游 | 50,000 | 每年 | 中 |
| **中期(3-10年)** | 大女儿高中/大学教育金 | 500,000 | 9年 | 高 |
| | 小女儿学前/小学教育金 | 300,000 | 15年 | 高 |
| | 汽车置换 | 300,000 | 5年 | 中 |
| **长期(>10年)** | 夫妻二人退休养老金 | 5,000,000 | 20年 | 高 |
| | 父母长期医疗/护理储备 | 500,000 | 随时 | 高 |
"""
    
    # 创建解析器并处理
    parser_obj = MarkdownTableParser()
    tables = parser_obj.parse_markdown_content(content)
    
    if not tables:
        print("没有找到有效的Markdown表格")
        sys.exit(1)
    
    print(f"找到 {len(tables)} 个表格")
    for i, table in enumerate(tables):
        print(f"  {i+1}. {table['title']}")
    
    # 转换为Excel
    parser_obj.tables_to_excel(tables, args.output)


if __name__ == "__main__":
    main()
