"""Pony ORM 数据库绑定；连接参数从环境变量读取（见 .env）。"""

import os

from dotenv import load_dotenv
from pony.orm import Database

load_dotenv()

db = Database()


def bind() -> None:
    """使用 MySQL（pymysql）连接，库名默认 ghcis（MOON_DB_NAME）。"""
    db.bind(
        provider="mysql",
        host=os.environ["MOON_DB_HOST"],
        port=int(os.environ.get("MOON_DB_PORT", "3306")),
        user=os.environ["MOON_DB_USER"],
        passwd=os.environ["MOON_DB_PASSWORD"],
        db=os.environ["MOON_DB_NAME"],
    )
