"""抓取 ghcis.com 毕业生去向——统一爬虫，支持 2025/2026 届。"""

from __future__ import annotations

import random
import re
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

ASHX_URL = "https://ghcis.com/ashx/showlist.ashx"

# 届别对应 yid
COHORT_YIDS: dict[int, str] = {2025: "290", 2026: "320"}

# 地区顺序（2025 动态获取 cid 时保持一致）
REGION_ORDER = ["英国", "美国", "加拿大", "中国香港", "其它"]

# 2026 固定 cid 映射
REGION_BY_CID_2026: dict[str, str] = {
    "321": "英国",
    "322": "美国",
    "323": "加拿大",
    "324": "中国香港",
    "325": "其它",
}
SKIP_CIDS_2026: frozenset[str] = frozenset({"322"})
SKIP_REGIONS_2025: frozenset[str] = frozenset({"美国"})

# 正则
_STUDENT_SUFFIX = re.compile(r"（\s*(\d+)\s*份\s*）\s*$")
_BLOCK = re.compile(
    r"(?:预录取\s*学生|入读学生|录取\s*学生)\s*[：:]\s*(.+?)"
    r"(?=(?:预录取\s*份数|入读份数|录取\s*份数)\s*[：:])",
    re.DOTALL,
)
_TOTAL = re.compile(
    r"(?:预录取\s*份数|入读份数|录取\s*份数)\s*[：:]\s*((?:\d\s*)+)",
    re.DOTALL,
)


def split_school(title: str) -> tuple[str, str]:
    """从校名块里切出中文名 + 英文名（从首个 ASCII 字母开始切）。"""
    title = title.strip()
    for i, c in enumerate(title):
        if c.isascii() and c.isalpha():
            return title[:i].strip(), title[i:].strip()
    return title, ""


def pair_title_detail_sections(root: Tag) -> list[tuple[Tag, Tag]]:
    """按文档顺序配对 106349（校名）与紧随其后的 106351（名单+份数）。"""
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


# ----------------------------------------------------------------------
# 2025 解析风格：找含中文逗号的最长 span 作为学生列表
# ----------------------------------------------------------------------
# 清理 token 里的字段名前缀（入读学生：、入读人数：、录取学生： 等及其换行变体）
_FIELD_PREFIX = re.compile(
    r"^(?:入读|预录取|录取)\s*学生\s*[：:]\s*"
    r"|^(?:入读|预录取|录取)\s*人数\s*[：:]\s*",
    re.DOTALL,
)


def _clean_token(p: str) -> str:
    """去掉字段名前缀和空白，保留人名或份数表达式。"""
    return _FIELD_PREFIX.sub("", p).strip()


def _parse_students_from_detail(detail_sec: Tag) -> tuple[list[tuple[str, int]], int | None]:
    spans = detail_sec.find_all("span")
    candidates: list[str] = []
    for sp in spans:
        tx = sp.get_text(strip=True)
        if tx and "、" in tx:
            candidates.append(tx)
    if not candidates:
        all_text = detail_sec.get_text("\n", strip=True)
        candidates = [all_text]
    blob = max(candidates, key=len)
    parts = [p.strip() for p in blob.split("、") if p.strip()]
    if not parts:
        return [], None
    students: list[tuple[str, int]] = []
    for p in parts:
        p = _clean_token(p)
        m = _STUDENT_SUFFIX.search(p)
        if m:
            name = p[: m.start()].strip()
            offers = int(m.group(1))
        else:
            name, offers = p, 1
        if name:
            students.append((name, offers))
    total = sum(n for _, n in students) if students else None
    return students, total


# ----------------------------------------------------------------------
# 2026 解析风格：正则匹配字段名
# ----------------------------------------------------------------------
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


# ----------------------------------------------------------------------
# 通用 HTML 解析（分派到对应解析风格）
# ----------------------------------------------------------------------
def _parse_region_html(
    html_fragment: str, region: str, cohort: int, scrape_date: str, use_2025_parser: bool
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(f"<div id='__root'>{html_fragment}</div>", "html.parser")
    root = soup.find(id="__root")
    if root is None:
        return []
    pairs = pair_title_detail_sections(root)
    rows: list[dict[str, Any]] = []
    for title_sec, detail_sec in pairs:
        school_cn, school_en = split_school(title_sec.get_text(" ", strip=True))
        if use_2025_parser:
            students, total = _parse_students_from_detail(detail_sec)
        else:
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
                    "scrape_date": scrape_date,
                }
            )
    return rows


# ----------------------------------------------------------------------
# 网络请求
# ----------------------------------------------------------------------
def _fetch_region_fragment(cid: str, yid: str) -> str:
    r = requests.post(
        ASHX_URL,
        data={"action": "getGraduateGo", "yid": yid, "cid": cid},
        timeout=120,
        headers={"User-Agent": "ghcis-scraper/1.0"},
    )
    r.raise_for_status()
    t = r.text.strip()
    if t == "1":
        return ""
    return t


# ----------------------------------------------------------------------
# 2025 届
# ----------------------------------------------------------------------
def fetch_all_2025_rows(scrape_date: str | None = None) -> list[dict[str, Any]]:
    """拉取 2025 届数据（动态获取地区 cid，跳过美国）。"""
    if scrape_date is None:
        scrape_date = date.today().isoformat()
    yid = COHORT_YIDS[2025]
    all_rows: list[dict[str, Any]] = []

    r = requests.post(
        ASHX_URL,
        data={"action": "getGraduateGoCountry", "yid": yid, "sid": random.random()},
        timeout=60,
        headers={"User-Agent": "ghcis-scraper/1.0"},
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
        if region in SKIP_REGIONS_2025:
            continue
        frag = _fetch_region_fragment(cid, yid)
        if not frag:
            continue
        all_rows.extend(
            _parse_region_html(frag, region=region, cohort=2025, scrape_date=scrape_date, use_2025_parser=True)
        )
    return all_rows


# ----------------------------------------------------------------------
# 2026 届
# ----------------------------------------------------------------------
def fetch_all_2026_rows(scrape_date: str | None = None) -> list[dict[str, Any]]:
    """拉取 2026 届数据（固定 cid 映射，跳过美国）。"""
    if scrape_date is None:
        scrape_date = date.today().isoformat()
    yid = COHORT_YIDS[2026]
    all_rows: list[dict[str, Any]] = []

    for cid, region in sorted(REGION_BY_CID_2026.items(), key=lambda x: x[0]):
        if cid in SKIP_CIDS_2026:
            continue
        frag = _fetch_region_fragment(cid, yid)
        if not frag:
            continue
        all_rows.extend(
            _parse_region_html(frag, region=region, cohort=2026, scrape_date=scrape_date, use_2025_parser=False)
        )
    return all_rows
