"""抓取 ghcis.com 毕业生去向（2025），按地区请求 ashx 接口并解析为行数据。

设计目标：独立于 2026 的 scraper 逻辑，不改动既有 2026 抓取脚本；
写入时仅使用 cohort=2025。
"""

from __future__ import annotations

import random
import re
from typing import Any

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

# 2025 在页面年份切换里对应 yid=290
COHORT_2025_YID = "290"
ASHX_URL = "https://ghcis.com/ashx/showlist.ashx"

# 页面 cid 在不同 yid（年份）下可能不同；必须先用 getGraduateGoCountry 动态拿 cid。
# 返回列表顺序看起来稳定为：英国、美国、加拿大、中国香港、其它
REGION_ORDER = ["英国", "美国", "加拿大", "中国香港", "其它"]
SKIP_REGIONS: frozenset[str] = frozenset({"美国"})

_STUDENT_SUFFIX = re.compile(r"（\s*(\d+)\s*份\s*）\s*$")


def _parse_students_from_detail(detail_sec: Tag) -> tuple[list[tuple[str, int]], int | None]:
    """从 2025 年的详情区块中抽取学生列表与总份数。

    2025 页面字段名（例如「入读学生/入读份数」）在当前抓取里可能出现编码/排版差异，
    导致依赖字段名的正则匹配不稳定。

    这里改为更稳的做法：
    - 找出 detail_sec 内包含中文逗号 `、` 的最长 span 文本，认为它就是“学生列表串”
    - 按 `、` 分割得到每个学生 token
    - 每个 token：
        - 若末尾匹配 `（n份）`，则 name + offer=n
        - 否则 offer=1
    - university_total_offers 取 `sum(student_offers)` 作为总份数（同时也和页面里的份数一致性更高）
    """
    spans = detail_sec.find_all("span")
    candidates: list[str] = []
    for sp in spans:
        tx = sp.get_text(strip=True)
        if tx and "、" in tx:
            candidates.append(tx)

    # 兜底：如果没找到明显包含分隔符的 span，则直接用整个可见文本
    if not candidates:
        all_text = detail_sec.get_text("\n", strip=True)
        candidates = [all_text]

    blob = max(candidates, key=len)
    parts = [p.strip() for p in blob.split("、") if p.strip()]
    if not parts:
        return [], None

    students: list[tuple[str, int]] = []
    for p in parts:
        m = _STUDENT_SUFFIX.search(p)
        if m:
            name = p[: m.start()].strip()
            offers = int(m.group(1))
        else:
            name, offers = p.strip(), 1
        if name:
            students.append((name, offers))

    total = sum(n for _, n in students) if students else None
    return students, total


def _split_school(title: str) -> tuple[str, str]:
    """从校名块里切出 中文名 + 英文名（从首个 ASCII 字母开始切）。"""
    title = title.strip()
    for i, c in enumerate(title):
        if c.isascii() and c.isalpha():
            return title[:i].strip(), title[i:].strip()
    return title, ""


def _parse_student_blob(blob: str) -> list[tuple[str, int]]:
    # 保留函数名但不再使用：2025 解析改为 _parse_students_from_detail（更稳）
    return [(m.group(0), 1)] if blob else []


def _parse_detail_block(text: str) -> tuple[list[tuple[str, int]], int | None]:
    # 保留函数名但不再使用：2025 解析改为 _parse_students_from_detail（更稳）
    return [], None


def _pair_title_detail_sections(root: Tag) -> list[tuple[Tag, Tag]]:
    """按文档顺序配对 106349（校名）与紧随其后的 106351（名单+份数）。

    美国区 HTML 里会多出一个无对应标题的 106351；即便本脚本跳过美国，
    仍沿用同样的配对策略，避免未来年份/地区版式差异导致错位。
    """
    pairs: list[tuple[Tag, Tag]] = []
    pending_title: Tag | None = None
    for sec in root.find_all("section"):
        did = sec.get("data-id")
        if did == "106349":
            pending_title = sec
        elif did == "106351" and pending_title is not None:
            pairs.append((pending_title, sec))
            pending_title = None
    return pairs


def parse_region_html(html_fragment: str, region: str, cohort: int) -> list[dict[str, Any]]:
    """将接口返回的 HTML 片段解析为入库字典列表。"""
    soup = BeautifulSoup(f"<div id='__root'>{html_fragment}</div>", "html.parser")
    root = soup.find(id="__root")
    if root is None:
        return []

    pairs = _pair_title_detail_sections(root)
    rows: list[dict[str, Any]] = []

    for title_sec, detail_sec in pairs:
        school_cn, school_en = _split_school(title_sec.get_text(" ", strip=True))
        students, total = _parse_students_from_detail(detail_sec)
        if total is None:
            continue
        for name, cnt in students:
            rows.append(
                {
                    "cohort": cohort,
                    "region": region,
                    "school_cn": school_cn,
                    "school_en": school_en,
                    "university_total_offers": total,
                    "student_name": name,
                    "student_offers": cnt,
                }
            )
    return rows


def fetch_region_fragment(cid: str) -> str:
    r = requests.post(
        ASHX_URL,
        data={"action": "getGraduateGo", "yid": COHORT_2025_YID, "cid": cid},
        timeout=120,
        headers={"User-Agent": "ghcis-scraper/2025.0"},
    )
    r.raise_for_status()
    t = r.text.strip()
    if t == "1":
        return ""
    return t


def fetch_all_2025_rows() -> list[dict[str, Any]]:
    """拉取 2025 地区并合并为行（不含 SKIP_REGIONS）。"""
    all_rows: list[dict[str, Any]] = []

    # 先动态获取 2025（yid=290）下五类地区的 cid
    r = requests.post(
        ASHX_URL,
        data={"action": "getGraduateGoCountry", "yid": COHORT_2025_YID, "sid": random.random()},
        timeout=60,
        headers={"User-Agent": "ghcis-scraper/2025.0"},
        verify=False,
    )
    r.raise_for_status()
    j = r.json()
    lst = j.get("list") or []
    if len(lst) < 5:
        raise RuntimeError(f"getGraduateGoCountry returned unexpected list length: {len(lst)}")

    region_by_cid: dict[str, str] = {}
    for i in range(5):
        cid = str(lst[i]["ID"])
        region_by_cid[cid] = REGION_ORDER[i]

    for cid, region in sorted(region_by_cid.items(), key=lambda x: x[0]):
        if region in SKIP_REGIONS:
            continue
        frag = fetch_region_fragment(cid)
        if not frag:
            continue
        all_rows.extend(parse_region_html(frag, region=region, cohort=2025))
    return all_rows

