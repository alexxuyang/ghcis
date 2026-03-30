"""创建按校汇总视图 v_cohort_2026_by_school（一次性/可重复执行）。"""

import os

import pymysql
from dotenv import load_dotenv

load_dotenv()

def _table_name(cur) -> str:
    cur.execute("SHOW TABLES LIKE 'admissionoffer'")
    if cur.fetchone():
        return "admissionoffer"
    cur.execute("SHOW TABLES LIKE 'AdmissionOffer'")
    if cur.fetchone():
        return "AdmissionOffer"
    raise RuntimeError("找不到 admissionoffer / AdmissionOffer 表")


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
            t = _table_name(cur)
            sql = f"""
CREATE OR REPLACE VIEW v_cohort_2026_by_school AS
SELECT
  cohort AS `届别`,
  region AS `地区`,
  school_cn AS `学校中文名`,
  school_en AS `学校英文名`,
  COUNT(*) AS `学生记录数`,
  SUM(student_offers) AS `学生offer合计`,
  MAX(university_total_offers) AS `页面公布该校总offer数`
FROM `{t}`
WHERE cohort = 2026
GROUP BY cohort, region, school_cn, school_en
"""
            cur.execute(sql)
        conn.commit()
        print("OK: view v_cohort_2026_by_school created (from table %s)" % t)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
