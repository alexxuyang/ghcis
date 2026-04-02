"""入口：爬取 2025/2026 届录取数据，存入 admissionoffer 表。"""

from __future__ import annotations

from datetime import date

import models  # noqa: F401 — 注册实体
from database import bind, db
from models import AdmissionOffer
from scraper import fetch_all_2025_rows, fetch_all_2026_rows


def main() -> None:
    bind()
    db.generate_mapping(create_tables=True)

    today = date.today().isoformat()

    print("Fetching 2025 data...")
    rows_2025 = fetch_all_2025_rows(scrape_date=today)
    print(f"2025 rows fetched: {len(rows_2025)}")

    print("Fetching 2026 data...")
    rows_2026 = fetch_all_2026_rows(scrape_date=today)
    print(f"2026 rows fetched: {len(rows_2026)}")

    # 2025：清空同 cohort 数据后重新写入
    with db_session:
        AdmissionOffer.select(lambda o: o.cohort == 2025).delete(bulk=True)
        for r in rows_2025:
            AdmissionOffer(**r)
        commit()

    # 2026：追加，不删除历史
    with db_session:
        for r in rows_2026:
            AdmissionOffer(**r)
        commit()

    with db_session:
        n5 = AdmissionOffer.select(lambda o: o.cohort == 2025).count()
        n6 = AdmissionOffer.select(lambda o: o.cohort == 2026).count()
    print(f"Rows inserted — 2025: {n5}, 2026: {n6}")


if __name__ == "__main__":
    main()
