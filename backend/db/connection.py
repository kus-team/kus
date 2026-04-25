"""
Подключение к БД с двумя режимами:
  - postgresql://...   → psycopg 3 (production / Neon)
  - пусто или sqlite:  → встроенный sqlite3 (local dev, чтобы не блокироваться)

API наружу одинаковый: connect() возвращает контекстный менеджер;
курсор отдаёт строки в виде dict (row['col'] и row.get('col')).
Плейсхолдеры в SQL: %(name)s — для совместимости в обоих режимах
(в sqlite автоматически переводятся в :name перед exec).
"""
from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from backend.config import DATABASE_URL

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SQLITE_PATH = ROOT / "data" / "kus.sqlite"


def _is_postgres(url: str) -> bool:
    return url.startswith("postgres://") or url.startswith("postgresql://")


# ============================================================
# SQLite adapter (имитирует мини-API psycopg, чтобы не дублировать код)
# ============================================================

_PARAM_RE = re.compile(r"%\((\w+)\)s")


def _to_sqlite_sql(sql: str) -> str:
    """Преобразует %(name)s → :name (psycopg-style → sqlite-style)."""
    return _PARAM_RE.sub(r":\1", sql).replace("%s", "?")


class _SqliteCursor:
    def __init__(self, raw_cur: sqlite3.Cursor):
        self._cur = raw_cur

    def execute(self, sql: str, params: Any = None):
        sql = _to_sqlite_sql(sql)
        if params is None:
            self._cur.execute(sql)
        else:
            self._cur.execute(sql, params)
        return self

    def executemany(self, sql: str, seq):
        sql = _to_sqlite_sql(sql)
        self._cur.executemany(sql, seq)
        return self

    def executescript(self, sql: str):
        self._cur.executescript(sql)
        return self

    def fetchone(self):
        row = self._cur.fetchone()
        return dict(row) if row else None

    def fetchall(self):
        return [dict(r) for r in self._cur.fetchall()]

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    def close(self):
        self._cur.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _SqliteConn:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(str(path), detect_types=sqlite3.PARSE_DECLTYPES)
        self._con.row_factory = sqlite3.Row
        self._con.execute("PRAGMA foreign_keys = ON")
        self._con.execute("PRAGMA journal_mode = WAL")

    def cursor(self) -> _SqliteCursor:
        return _SqliteCursor(self._con.cursor())

    def commit(self):
        self._con.commit()

    def rollback(self):
        self._con.rollback()

    def close(self):
        self._con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *exc):
        if exc_type is None:
            try:
                self.commit()
            except Exception:
                pass
        self.close()


# ============================================================
# Postgres connection (psycopg)
# ============================================================

def _connect_postgres(url: str):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(url, row_factory=dict_row, autocommit=False)


# ============================================================
# Public API
# ============================================================

def connect():
    if DATABASE_URL and _is_postgres(DATABASE_URL):
        return _connect_postgres(DATABASE_URL)
    # default: SQLite
    return _SqliteConn(DEFAULT_SQLITE_PATH)


def schema_file() -> Path:
    """Какой schema-файл применять, в зависимости от диалекта."""
    if DATABASE_URL and _is_postgres(DATABASE_URL):
        return Path(__file__).parent / "schema.sql"
    return Path(__file__).parent / "schema_sqlite.sql"


def apply_schema() -> None:
    sql = schema_file().read_text(encoding="utf-8")
    con = connect()
    try:
        cur = con.cursor()
        # SQLite executescript для multi-statement; psycopg execute проглотит как есть
        if isinstance(con, _SqliteConn):
            cur.executescript(sql)
        else:
            cur.execute(sql)
        con.commit()
    finally:
        con.close()


def dialect() -> str:
    return "postgres" if (DATABASE_URL and _is_postgres(DATABASE_URL)) else "sqlite"


if __name__ == "__main__":
    print(f"dialect: {dialect()}")
    print(f"schema:  {schema_file()}")
    apply_schema()
    print("schema applied")
