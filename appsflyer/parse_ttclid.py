#!/usr/bin/env python3
"""
从一批 AppsFlyer Raw Data 文件中解析 ttclid 并导出到 ttclid.txt。

支持：
- 直接传入 CSV 文件
- 传入包含 CSV 的目录（可选递归）
- 传入包含 CSV 的 ZIP 文件（AppsFlyer 常见打包形式）

用法示例：
  python parse_ttclid.py data/*.csv
  python parse_ttclid.py data --recursive -o out_ttclid.txt
  python parse_ttclid.py ~/Downloads/af_raw.zip

说明（按你的需求已收敛）：
- 仅从列名为 Original URL 的字段中解析 URL 查询参数；
- 从中提取参数名为 ttclid 的值（大小写不敏感）；
- 默认去重并保持首次出现顺序；
- 输出文件每行一个 ttclid。
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from typing import Iterable, Iterator, List, Sequence, Tuple

try:
    import zipfile
except Exception:  # pragma: no cover - 标准库应当存在
    zipfile = None  # type: ignore


TTCLID_PATTERNS = [
    # URL 查询参数场景：?ttclid=XXX 或 &ttclid=XXX
    re.compile(r"[?&]ttclid=([A-Za-z0-9_\-]+)", re.IGNORECASE),
    # 类 JSON 文本："ttclid":"XXX" 或 'ttclid':'XXX'
    re.compile(r"[\"']ttclid[\"']\s*[:=]\s*[\"']([A-Za-z0-9_\-]+)[\"']", re.IGNORECASE),
    # 宽松兜底：ttclid=XXX（避免与上面重复，放最后）
    re.compile(r"\bttclid\s*[:=]\s*([A-Za-z0-9_\-]+)", re.IGNORECASE),
]

# 常见可能包含点击 URL/参数的列名（不区分大小写）
ORIGINAL_URL_FIELD_CANDIDATES = [
    "original url",  # AppsFlyer 标准字段
]


def extract_ttclids_from_text(text: str) -> List[str]:
    if not text:
        return []
    found: List[str] = []
    for pat in TTCLID_PATTERNS:
        for m in pat.findall(text):
            if m and m not in found:
                found.append(m)
    return found


def _iter_paths(paths: Sequence[str], recursive: bool) -> Iterator[Tuple[str, str]]:
    """遍历输入路径，产出 (kind, path)。

    kind: 'csv' | 'zip' | 'other'
    - csv: 直接的 CSV 文件
    - zip: ZIP 包
    - other: 其他文件（也尝试按 CSV 读）
    目录会展开其中的文件，受 recursive 控制。
    """
    for p in paths:
        if os.path.isdir(p):
            if recursive:
                for root, _, files in os.walk(p):
                    for f in files:
                        fp = os.path.join(root, f)
                        lower = f.lower()
                        if lower.endswith(".zip"):
                            yield ("zip", fp)
                        elif lower.endswith(".csv"):
                            yield ("csv", fp)
                        else:
                            yield ("other", fp)
            else:
                for f in os.listdir(p):
                    fp = os.path.join(p, f)
                    if not os.path.isfile(fp):
                        continue
                    lower = f.lower()
                    if lower.endswith(".zip"):
                        yield ("zip", fp)
                    elif lower.endswith(".csv"):
                        yield ("csv", fp)
                    else:
                        yield ("other", fp)
        else:
            lower = p.lower()
            if lower.endswith(".zip"):
                yield ("zip", p)
            elif lower.endswith(".csv"):
                yield ("csv", p)
            else:
                yield ("other", p)


def _iter_rows_from_csv_file(path: str) -> Iterator[Tuple[Sequence[str], dict | None]]:
    """从磁盘 CSV 文件读取行。

    返回迭代器，每项是 (row, row_dict)；当是 DictReader 时 row_dict 有值，
    普通 reader 时 row_dict 为 None，row 是列表。
    """
    with open(path, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        # 先尝试 DictReader，若没有表头会自动作为第一行字段名使用
        try:
            sample = f.read(4096)
        except Exception:
            sample = ""
        f.seek(0)
        sniffed = None
        try:
            sniffed = csv.Sniffer().sniff(sample)
        except Exception:
            pass
        reader: Iterable
        if sniffed:
            reader = csv.DictReader(f, dialect=sniffed)
        else:
            reader = csv.DictReader(f)

        if isinstance(reader, csv.DictReader):
            for row in reader:
                yield (list(row.values()) if row else [], row)
        else:
            f.seek(0)
            reader2 = csv.reader(f)
            for row in reader2:
                yield (row, None)


def _iter_rows_from_zip(path: str) -> Iterator[Tuple[Sequence[str], dict | None]]:
    if zipfile is None:
        return
    try:
        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                name = info.filename
                if not name.lower().endswith(".csv"):
                    continue
                with zf.open(info, "r") as rf:
                    # 转成文本 IO
                    import io

                    tf = io.TextIOWrapper(rf, encoding="utf-8-sig", errors="ignore", newline="")
                    reader = csv.DictReader(tf)
                    if isinstance(reader, csv.DictReader):
                        for row in reader:
                            yield (list(row.values()) if row else [], row)
                    else:
                        tf.seek(0)
                        reader2 = csv.reader(tf)
                        for row in reader2:
                            yield (row, None)
    except zipfile.BadZipFile:
        # 非法 zip，当作普通文件处理
        for x in _iter_rows_from_csv_file(path):
            yield x


def iter_rows(paths: Sequence[str], recursive: bool) -> Iterator[Tuple[Sequence[str], dict | None]]:
    for kind, p in _iter_paths(paths, recursive):
        if kind == "zip":
            for row in _iter_rows_from_zip(p):
                yield row
        elif kind in ("csv", "other"):
            # 对于 other 也尝试 CSV 读取
            try:
                for row in _iter_rows_from_csv_file(p):
                    yield row
            except Exception:
                # 读取失败则跳过
                continue


def normalize_fieldnames(row_dict: dict | None) -> Tuple[dict | None, dict]:
    """返回 (原字典, 小写->原名 的映射)."""
    if not row_dict:
        return (row_dict, {})
    mapping = {k.lower(): k for k in row_dict.keys()}
    return (row_dict, mapping)


def extract_from_row(row: Sequence[str], row_dict: dict | None) -> List[str]:
    """仅从 Original URL 字段中提取 ttclid。"""
    if row_dict is None:
        return []

    _, mapping = normalize_fieldnames(row_dict)
    # 找到 Original URL 字段（不区分大小写）
    field_key = None
    for cand in ORIGINAL_URL_FIELD_CANDIDATES:
        if cand in mapping:
            field_key = mapping[cand]
            break
    if field_key is None:
        return []

    url_val = row_dict.get(field_key, "")
    if not isinstance(url_val, str) or not url_val:
        return []

    # 使用 urllib 解析查询参数
    from urllib.parse import urlparse, parse_qs

    try:
        parsed = urlparse(url_val)
    except Exception:
        parsed = None

    hits: List[str] = []
    if parsed is not None:
        qs = parse_qs(parsed.query, keep_blank_values=False)
        # 大小写不敏感访问
        for k, v in qs.items():
            if k.lower() == "ttclid":
                # parse_qs 返回 list[str]
                for item in v:
                    if item and item not in hits:
                        hits.append(item)

    # 兜底：若 URL 非法但文本中含有 ttclid=，再用正则尝试一次
    if not hits:
        for m in extract_ttclids_from_text(url_val):
            if m not in hits:
                hits.append(m)
    return hits


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="解析 AppsFlyer Raw Data 文件中的 ttclid 并导出")
    p.add_argument("paths", nargs="+", help="CSV/ZIP 文件或目录，可多个")
    p.add_argument("-o", "--output", default="ttclid.txt", help="输出文件名，默认 ttclid.txt")
    p.add_argument("-r", "--recursive", action="store_true", help="目录递归扫描")
    p.add_argument("--no-dedupe", action="store_true", help="不去重，保留全部出现")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    ns = parse_args(sys.argv[1:] if argv is None else argv)
    ttclids: List[str] = []
    seen = set()
    rows_scanned = 0
    files_note = ", ".join(ns.paths)
    print(f"开始解析：{files_note}")

    for row, row_dict in iter_rows(ns.paths, ns.recursive):
        rows_scanned += 1
        hits = extract_from_row(row, row_dict)
        if not hits:
            continue
        if ns.no_dedupe:
            ttclids.extend(hits)
        else:
            for h in hits:
                if h not in seen:
                    seen.add(h)
                    ttclids.append(h)

    # 写入输出
    with open(ns.output, "w", encoding="utf-8") as wf:
        for item in ttclids:
            wf.write(str(item).strip() + "\n")

    print(f"完成。共扫描 {rows_scanned} 行，提取到 {len(ttclids)} 个 ttclid。输出：{ns.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
