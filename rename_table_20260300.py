"""将 admissionoffer 重命名为 admissionoffer_20260300（一次性脚本）。"""

import os

import pymysql
from dotenv import load_dotenv


def _find_source_table(cur) -> str:
    cur.execute("SHOW TABLES LIKE 'admissionoffer'")
    if cur.fetchone():
        return "admissionoffer"
    cur.execute("SHOW TABLES LIKE 'AdmissionOffer'")
    if cur.fetchone():
        return "AdmissionOffer"
    raise RuntimeError("找不到 admissionoffer / AdmissionOffer 表")


def _target_exists(cur, target: str) -> bool:
    cur.execute("SHOW TABLES LIKE %s", (target,))
    return cur.fetchone() is not None


def main() -> None:
    load_dotenv()
    target = "admissionoffer_20260300"

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
            src = _find_source_table(cur)
            if _target_exists(cur, target):
                raise RuntimeError(f"目标表已存在：{target}")
            cur.execute(f"RENAME TABLE `{src}` TO `{target}`")
        conn.commit()
        print(f"OK: renamed {src} -> {target}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

