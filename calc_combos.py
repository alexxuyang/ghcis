"""按指定学校组合统计去重学生人数（COUNT(DISTINCT student_name)）。"""

from __future__ import annotations

import os

import pymysql
from dotenv import load_dotenv

load_dotenv()

COHORTS = (2025, 2026)

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
        "牛津+剑桥+IC+UCL+港大+曼大+KCL+爱丁堡+港科+多伦多",
        ["牛津大学", "剑桥大学", "帝国理工学院", "伦敦大学学院", "香港大学", "曼彻斯特大学", "伦敦国王学院", "爱丁堡大学", "香港科技大学", "多伦多大学"],
    ),
]

TABLE = "admissionoffer"


def _count_distinct_by_school_substrings(cur, cohort: int, substrings: list[str]) -> int:
    """组合里用 school_cn 子串匹配（LIKE），取最新一次爬取的数据。"""
    where_parts = []
    params: list[object] = []
    for s in substrings:
        where_parts.append("school_cn LIKE %s")
        params.append(f"%{s}%")
    where_sql = " OR ".join(where_parts)

    sql = f"""
        SELECT COUNT(DISTINCT student_name) AS cnt
        FROM {TABLE} t1
        WHERE cohort = %s
          AND scrape_date = (
              SELECT MAX(scrape_date) FROM {TABLE} WHERE cohort = %s
          )
          AND ({where_sql})
    """
    params = [cohort, cohort] + params
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row[0] if row else 0)


def _count_total_students(cur, cohort: int) -> int:
    sql = f"""
        SELECT COUNT(DISTINCT student_name) AS cnt
        FROM {TABLE} t1
        WHERE cohort = %s
          AND scrape_date = (
              SELECT MAX(scrape_date) FROM {TABLE} WHERE cohort = %s
          )
    """
    cur.execute(sql, (cohort, cohort))
    row = cur.fetchone()
    return int(row[0] if row else 0)


def main() -> None:
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
            totals = {c: _count_total_students(cur, c) for c in COHORTS}
            print("组合\t2025%\t2026%")
            for label, schools in COMBOS:
                c25 = _count_distinct_by_school_substrings(cur, 2025, schools)
                c26 = _count_distinct_by_school_substrings(cur, 2026, schools)
                p25 = round(c25 / totals[2025] * 100, 1) if totals[2025] else 0.0
                p26 = round(c26 / totals[2026] * 100, 1) if totals[2026] else 0.0
                print(f"{label}\t{p25}%\t{p26}%")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
