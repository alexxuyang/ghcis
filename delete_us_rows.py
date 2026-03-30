"""删除库中 region='美国' 的 AdmissionOffer 行（一次性脚本）。"""

import os

import pymysql
from dotenv import load_dotenv

load_dotenv()


def main() -> None:
    conn = pymysql.connect(
        host=os.environ["MOON_DB_HOST"],
        port=int(os.environ.get("MOON_DB_PORT", "3306")),
        user=os.environ["MOON_DB_USER"],
        password=os.environ["MOON_DB_PASSWORD"],
        database=os.environ["MOON_DB_NAME"],
        charset="utf8mb4",
    )
    for table in ("admissionoffer", "AdmissionOffer"):
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM `{table}` WHERE `region` = %s", ("美国",))
                n = cur.rowcount
            conn.commit()
            print(f"OK: deleted {n} rows from {table}")
            return
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e).lower() or "1146" in str(e):
                continue
            raise
    raise RuntimeError("找不到 admissionoffer / AdmissionOffer 表")


if __name__ == "__main__":
    main()
