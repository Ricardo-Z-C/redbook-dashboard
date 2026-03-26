# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/config/db_config.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#

# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。


import os
from urllib.parse import parse_qs, unquote, urlparse


def _parse_mysql_jdbc_url(raw_url: str):
    value = str(raw_url or "").strip()
    if not value:
        return {}

    normalized = value
    if normalized.startswith("jdbc:"):
        normalized = normalized[len("jdbc:"):]

    try:
        parsed = urlparse(normalized)
    except Exception:
        return {}

    if parsed.scheme != "mysql":
        return {}

    query = parse_qs(parsed.query or "")
    db_name = str(parsed.path or "").lstrip("/")
    user = unquote(str(parsed.username or ""))
    password = unquote(str(parsed.password or ""))

    if query.get("user"):
        user = unquote(str(query["user"][0] or ""))
    if query.get("password"):
        password = unquote(str(query["password"][0] or ""))

    return {
        "host": str(parsed.hostname or ""),
        "port": int(parsed.port or 3306),
        "db_name": db_name,
        "user": user,
        "password": password,
    }

# mysql config
_jdbc_overrides = _parse_mysql_jdbc_url(
    os.getenv("MYSQL_JDBC_URL", "") or os.getenv("COMPETITION_JDBC_URL", "")
)
MYSQL_DB_PWD = os.getenv("MYSQL_DB_PWD", _jdbc_overrides.get("password", "123456"))
MYSQL_DB_USER = os.getenv("MYSQL_DB_USER", _jdbc_overrides.get("user", "root"))
MYSQL_DB_HOST = os.getenv("MYSQL_DB_HOST", _jdbc_overrides.get("host", "localhost"))
MYSQL_DB_PORT = os.getenv("MYSQL_DB_PORT", _jdbc_overrides.get("port", 3306))
MYSQL_DB_NAME = os.getenv("MYSQL_DB_NAME", _jdbc_overrides.get("db_name", "media_crawler"))

mysql_db_config = {
    "user": MYSQL_DB_USER,
    "password": MYSQL_DB_PWD,
    "host": MYSQL_DB_HOST,
    "port": MYSQL_DB_PORT,
    "db_name": MYSQL_DB_NAME,
}


# redis config
REDIS_DB_HOST = os.getenv("REDIS_DB_HOST", "127.0.0.1")  # your redis host
REDIS_DB_PWD = os.getenv("REDIS_DB_PWD", "123456")  # your redis password
REDIS_DB_PORT = os.getenv("REDIS_DB_PORT", 6379)  # your redis port
REDIS_DB_NUM = os.getenv("REDIS_DB_NUM", 0)  # your redis db num

# cache type
CACHE_TYPE_REDIS = "redis"
CACHE_TYPE_MEMORY = "memory"

# sqlite config
SQLITE_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "sqlite_tables.db")

sqlite_db_config = {
    "db_path": SQLITE_DB_PATH
}

# mongodb config
MONGODB_HOST = os.getenv("MONGODB_HOST", "localhost")
MONGODB_PORT = os.getenv("MONGODB_PORT", 27017)
MONGODB_USER = os.getenv("MONGODB_USER", "")
MONGODB_PWD = os.getenv("MONGODB_PWD", "")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "media_crawler")

mongodb_config = {
    "host": MONGODB_HOST,
    "port": int(MONGODB_PORT),
    "user": MONGODB_USER,
    "password": MONGODB_PWD,
    "db_name": MONGODB_DB_NAME,
}

# postgres config
POSTGRES_DB_PWD = os.getenv("POSTGRES_DB_PWD", "123456")
POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER", "postgres")
POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST", "localhost")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT", 5432)
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME", "media_crawler")

postgres_db_config = {
    "user": POSTGRES_DB_USER,
    "password": POSTGRES_DB_PWD,
    "host": POSTGRES_DB_HOST,
    "port": POSTGRES_DB_PORT,
    "db_name": POSTGRES_DB_NAME,
}
