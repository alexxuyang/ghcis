"""按指定学校组合统计去重学生人数（COUNT(DISTINCT student_name)）。"""

from __future__ import annotations

import os

import pymysql
from dotenv import load_dotenv

load_dotenv()

COHORT = 2026
TOTAL_PEOPLE = 554  # 你的基准总人数

# 你需要的组合（逐步增加）
COMBOS: list[tuple[str, list[str]]] = [
    ("牛津+剑桥", ["牛津大学", "剑桥大学"]),
    ("牛津+剑桥+IC", ["牛津大学", "剑桥大学", "帝国理工学院"]),
    ("牛津+剑桥+IC+UCL", ["牛津大学", "剑桥大学", "帝国理工学院", "伦敦大学学院"]),
    ("牛津+剑桥+IC+UCL+港大", ["牛津大学", "剑桥大学", "帝国理工学院", "伦敦大学学院", "香港大学"]),
    (
        "牛津+剑桥+IC+UCL+港大+曼大+KCL+爱丁堡",
        ["牛津大学", "剑桥大学", "帝国理工学院", "伦敦大学学院", "香港大学", "曼彻斯特大学", "伦敦国王学院", "爱丁堡大学"],
    ),
    (
        "牛津+剑桥+IC+UCL+曼大+KCL+爱丁堡+港大+港科+多伦多",
        ["牛津大学", "剑桥大学", "帝国理工学院", "伦敦大学学院", "香港大学", "曼彻斯特大学", "伦敦国王学院", "爱丁堡大学", "香港科技大学", "多伦多大学"],
    ),
]


def _detect_table(cur) -> str:
    # 兼容 Pony 默认的大小写/你手动建库的表名
    cur.execute("SHOW TABLES LIKE 'admissionoffer'")
    if cur.fetchone():
        return "admissionoffer"
    cur.execute("SHOW TABLES LIKE 'AdmissionOffer'")
    if cur.fetchone():
        return "AdmissionOffer"
    raise RuntimeError("找不到 admissionoffer / AdmissionOffer 表")


def _count_distinct_by_school_substrings(cur, substrings: list[str]) -> int:
    """
    组合里用 school_cn 子串匹配（LIKE），避免中文/空格细微差异导致匹配不到。
    """
    where_parts = []
    params: list[object] = []
    for s in substrings:
        where_parts.append("school_cn LIKE %s")
        params.append(f"%{s}%")
    where_sql = " OR ".join(where_parts)
    sql = f"""
        SELECT COUNT(DISTINCT student_name) AS cnt
        FROM {TABLE}
        WHERE cohort = %s AND ({where_sql})
    """
    params = [COHORT] + params
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row[0] if row else 0)


TABLE: str


def main() -> None:
    global TABLE
    conn = pymysql.connect(
        host=os.environ["MOON_DB_HOST"],
        port=int(os.environ.get("MOON_DB_PORT", "3306")),
        user=os.environ["MOON_DB_USER"],
        password=os.environ["MOON_DB_PASSWORD"],
        database=os.environ["MOON_DB_NAME"],
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            TABLE = _detect_table(cur)
            for label, schools in COMBOS:
                cnt = _count_distinct_by_school_substrings(cur, schools)
                pct = round(cnt / TOTAL_PEOPLE * 100, 1) if TOTAL_PEOPLE else 0.0
                print(f"{label}\t{cnt}\t{pct}%")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
