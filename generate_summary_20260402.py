"""从 admissionoffer 表生成汇总文档。"""

import os

import pymysql
from dotenv import load_dotenv

load_dotenv()

COHORTS = (2025, 2026)

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


def get_conn():
    return pymysql.connect(
        host=os.environ["MOON_DB_HOST"],
        port=int(os.environ.get("MOON_DB_PORT", "3306")),
        user=os.environ["MOON_DB_USER"],
        password=os.environ["MOON_DB_PASSWORD"],
        database=os.environ["MOON_DB_NAME"],
        charset="utf8mb4",
    )


def count_total_students(cur, cohort: int) -> int:
    cur.execute(f"SELECT COUNT(DISTINCT student_name) FROM {TABLE} WHERE cohort=%s", (cohort,))
    row = cur.fetchone()
    return int(row[0] if row else 0)


def count_combo(cur, cohort: int, substrings: list[str]) -> int:
    where_parts = []
    params = []
    for s in substrings:
        where_parts.append("school_cn LIKE %s")
        params.append(f"%{s}%")
    where_sql = " OR ".join(where_parts)
    sql = f"""
        SELECT COUNT(DISTINCT student_name) AS cnt
        FROM {TABLE}
        WHERE cohort = %s AND ({where_sql})
    """
    params = [cohort] + params
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row[0] if row else 0)


def school_summary(cur, cohort: int):
    sql = f"""
        SELECT
          cohort,
          region,
          school_cn,
          school_en,
          COUNT(*) AS student_records,
          SUM(student_offers) AS total_offers,
          MAX(university_total_offers) AS page_total
        FROM {TABLE}
        WHERE cohort = %s
        GROUP BY cohort, region, school_cn, school_en
        ORDER BY student_records DESC
    """
    cur.execute(sql, (cohort,))
    return cur.fetchall()


def generate_combo_summary():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            totals = {c: count_total_students(cur, c) for c in COHORTS}

            lines = []
            lines.append("## 统计口径")
            lines.append("")
            lines.append("- **抓取时间**: 2026年4月2日")
            lines.append("- **去重口径**：按 `student_name` 去重（同一人拿多校只算 1 人）")
            lines.append("- **数据范围**：`admissionoffer` 表，`cohort in (2025, 2026)`")
            lines.append("- **地区**：当前配置为不抓取美国")
            lines.append("- **总人数（去重后）**：")
            lines.append(f"  - **2025**：{totals[2025]}")
            lines.append(f"  - **2026**：{totals[2026]}")
            lines.append("")
            lines.append("## 组合覆盖率（2025 / 2026）")
            lines.append("")
            lines.append("| 组合 | 2025录取率 | 2026预录取率 |")
            lines.append("|---|---:|---:|")
            for label, schools in COMBOS:
                c25 = count_combo(cur, 2025, schools)
                c26 = count_combo(cur, 2026, schools)
                p25 = round(c25 / totals[2025] * 100, 1) if totals[2025] else 0.0
                p26 = round(c26 / totals[2026] * 100, 1) if totals[2026] else 0.0
                lines.append(f"| {label} | {p25}% | {p26}% |")
            lines.append("")
            lines.append("## 学校匹配说明")
            lines.append("")
            lines.append("- **牛津**：牛津大学")
            lines.append("- **剑桥**：剑桥大学")
            lines.append("- **IC**：帝国理工学院")
            lines.append("- **UCL**：伦敦大学学院")
            lines.append("- **港大**：香港大学")
            lines.append("- **港科**：香港科技大学")
            lines.append("- **曼大**：曼彻斯特大学")
            lines.append("- **KCL**：伦敦国王学院")
            lines.append("- **爱丁堡**：爱丁堡大学")
            lines.append("- **多伦多**：多伦多大学")

            content = "\n".join(lines)
            with open("2025-2026-summary-20260402.md", "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Written: 2025-2026-summary-20260402.md")
    finally:
        conn.close()


def generate_school_summary():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            rows = school_summary(cur, 2026)

            lines = []
            lines.append("## 2026 按学校汇总（按学生记录数降序）")
            lines.append("")
            lines.append("- 数据来源：`admissionoffer` 表")
            lines.append("- 过滤条件：`cohort = 2026`")
            lines.append("")
            lines.append("| 届别 | 地区 | 学校中文名 | 学校英文名 | 学生记录数 | 学生offer合计 | 页面公布该校总offer数 |")
            lines.append("|---:|---|---|---|---:|---:|---:|")
            for row in rows:
                (cohort, region, school_cn, school_en, student_records, total_offers, page_total) = row
                school_en = school_en or ""
                lines.append(f"| {cohort} | {region} | {school_cn} | {school_en} | {student_records} | {total_offers} | {page_total} |")

            content = "\n".join(lines)
            with open("2026-school-summary-20260402.md", "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Written: 2026-school-summary-20260402.md")
    finally:
        conn.close()


if __name__ == "__main__":
    generate_combo_summary()
    generate_school_summary()
