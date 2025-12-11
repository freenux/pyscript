# Markdown表格转Excel工具

将Markdown格式的表格转换为Excel文件，每个表格放在不同的工作表中。

## 安装依赖

```bash
pip install pandas openpyxl
```

## 使用方法

### 1. 直接运行（使用内置示例数据）
```bash
python md_to_excel.py
```
这将使用脚本中内置的家庭财务表格示例数据，生成 `output.xlsx` 文件。

### 2. 处理自定义Markdown文件
```bash
python md_to_excel.py input.md -o output.xlsx
```

### 3. 命令行参数
- `input_file`: 输入的Markdown文件路径（可选）
- `-o, --output`: 输出的Excel文件路径（默认为 `output.xlsx`）

## 支持的Markdown表格格式

脚本支持标准的Markdown表格格式：

```markdown
### 表格标题

| 列1 | 列2 | 列3 |
| :--- | :--- | :--- |
| 数据1 | 数据2 | 数据3 |
| 数据4 | 数据5 | 数据6 |
```

## 功能特性

- 自动识别Markdown文件中的所有表格
- 提取表格前的标题作为Excel工作表名称
- 处理包含特殊字符的表格内容
- 自动清理Excel工作表名称（移除不支持的字符）
- 支持空单元格和格式化文本（如粗体标记）

## 示例输出

运行脚本后，会创建一个Excel文件，包含以下工作表：
1. 家庭资产负债表 (截至 YYYY年MM月DD日)_1
2. 家庭年度收入支出表 (YYYY年度)_2  
3. 家庭理财目标表_3

每个工作表都保持原始表格的结构和数据。 