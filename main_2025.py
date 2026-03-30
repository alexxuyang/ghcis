"""入口：同步 2025 届录取数据到数据库（只影响 cohort=2025）。"""

from __future__ import annotations

from pony.orm import commit, db_session, select

import models  # noqa: F401 — 注册实体
from database import bind, db
from models import AdmissionOffer
from scraper_2025 import fetch_all_2025_rows


def main() -> None:
    bind()
    db.generate_mapping(create_tables=True)

    rows = fetch_all_2025_rows()
    with db_session:
        AdmissionOffer.select(lambda o: o.cohort == 2025).delete(bulk=True)
        for r in rows:
            AdmissionOffer(**r)
        commit()

    with db_session:
        n = select(o for o in AdmissionOffer if o.cohort == 2025).count()
    print(f"2025 rows inserted: {n}")


if __name__ == "__main__":
    main()

