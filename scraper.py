"""抓取 ghcis.com 毕业生去向（2026），按地区请求 ashx 接口并解析为行数据。"""

from __future__ import annotations

import re
from typing import Any

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

COHORT_2026_YID = "320"
ASHX_URL = "https://ghcis.com/ashx/showlist.ashx"

# 页面 cid -> 库内地区（五类）
REGION_BY_CID: dict[str, str] = {
    "321": "英国",
    "322": "美国",
    "323": "加拿大",
    "324": "中国香港",
    "325": "其它",
}

# 暂不抓取美国（先不入库）
SKIP_CIDS: frozenset[str] = frozenset({"322"})

_STUDENT_SUFFIX = re.compile(r"（\s*(\d+)\s*份\s*）\s*$")
# 英国等多用「预录取学生/预录取份数」；美国等板块用「录取学生/录取份数」（无「预」字）
# TOTAL 中须把「预录取份数」写在「录取份数」之前，避免后者匹配到前者的后缀
_BLOCK = re.compile(
    r"(?:预录取\s*学生|入读学生|录取\s*学生)\s*[：:]\s*(.+?)"
    r"(?=(?:预录取\s*份数|入读份数|录取\s*份数)\s*[：:])",
    re.DOTALL,
)
# 份数可能被拆成多个标签，如 <strong>3</strong><strong>1</strong> → 文本「：\n3\n1」，不能只取第一段 \d+
_TOTAL = re.compile(
    r"(?:预录取\s*份数|入读份数|录取\s*份数)\s*[：:]\s*((?:\d\s*)+)",
    re.DOTALL,
)


def _split_school(title: str) -> tuple[str, str]:
    title = title.strip()
    for i, c in enumerate(title):
        if c.isascii() and c.isalpha():
            return title[:i].strip(), title[i:].strip()
    return title, ""


def _parse_student_blob(blob: str) -> list[tuple[str, int]]:
    blob = re.sub(r"\s+", "", blob)
    if not blob:
        return []
    parts = [p for p in blob.split("、") if p.strip()]
    out: list[tuple[str, int]] = []
    for p in parts:
        m = _STUDENT_SUFFIX.search(p)
        if m:
            name = p[: m.start()].strip()
            n = int(m.group(1))
        else:
            name, n = p.strip(), 1
        if name:
            out.append((name, n))
    return out


def _parse_detail_block(text: str) -> tuple[list[tuple[str, int]], int | None]:
    text = text.replace("\xa0", " ")
    m_block = _BLOCK.search(text)
    m_total = _TOTAL.search(text)
    if not m_block or not m_total:
        return [], None
    students = _parse_student_blob(m_block.group(1))
    digits_only = "".join(c for c in m_total.group(1) if c.isdigit())
    if not digits_only:
        return [], None
    total = int(digits_only)
    return students, total


def _pair_title_detail_sections(root: Tag) -> list[tuple[Tag, Tag]]:
    """按文档顺序配对 106349（校名）与紧随其后的 106351（名单+份数）。

    美国区 HTML 里会多出一个无对应标题的 106351；若用两个 select 再按索引 zip，
    会从第一行起整体错位（例如尔湾显示戴维斯的人数）。
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
        detail_text = detail_sec.get_text("\n", strip=True)
        students, total = _parse_detail_block(detail_text)
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
        data={"action": "getGraduateGo", "yid": COHORT_2026_YID, "cid": cid},
        timeout=120,
        headers={"User-Agent": "ghcis-scraper/0.1"},
    )
    r.raise_for_status()
    t = r.text.strip()
    if t == "1":
        return ""
    return t


def fetch_all_2026_rows() -> list[dict[str, Any]]:
    """拉取 2026 地区并合并为行（不含 SKIP_CIDS，当前跳过美国）。"""
    all_rows: list[dict[str, Any]] = []
    for cid, region in sorted(REGION_BY_CID.items(), key=lambda x: x[0]):
        if cid in SKIP_CIDS:
            continue
        frag = fetch_region_fragment(cid)
        if not frag:
            continue
        all_rows.extend(parse_region_html(frag, region=region, cohort=2026))
    return all_rows
