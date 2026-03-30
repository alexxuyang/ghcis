"""在 MySQL 中创建 v_cohort_2026_student_count 视图（一次性脚本）。"""

import os

import pymysql
from dotenv import load_dotenv

load_dotenv()

SQL = """
CREATE OR REPLACE VIEW v_cohort_2026_student_count AS
SELECT COUNT(DISTINCT student_name) AS 学生数_按姓名去重
FROM AdmissionOffer
WHERE cohort = 2026
"""


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
            cur.execute(SQL)
        conn.commit()
        print("OK: view v_cohort_2026_student_count created")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
