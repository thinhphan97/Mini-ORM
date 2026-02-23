"""Concrete SQL dialect implementations for DB-API adapters."""

from __future__ import annotations

from typing import Any, Optional


class Dialect:
    """Base dialect that defines SQL quoting and placeholder behavior."""

    name: str = "generic"
    paramstyle: str = "named"
    quote_char: str = '"'
    supports_returning: bool = False

    def q(self, ident: str) -> str:
        """Quote SQL identifier."""

        return f"{self.quote_char}{ident}{self.quote_char}"

    def placeholder(self, key: str) -> str:
        """Return parameter placeholder for current param style."""

        if self.paramstyle == "named":
            return f":{key}"
        if self.paramstyle == "qmark":
            return "?"
        if self.paramstyle == "format":
            return "%s"
        raise ValueError(f"Unsupported paramstyle: {self.paramstyle}")

    def auto_pk_sql(self, pk_name: str) -> str:
        """Return SQL fragment for auto-increment primary key column."""

        return f"{self.q(pk_name)} INTEGER PRIMARY KEY"

    def returning_clause(self, pk_name: str) -> str:
        """Return `RETURNING` clause when dialect supports it."""

        if self.supports_returning:
            return f" RETURNING {self.q(pk_name)}"
        return ""

    def get_lastrowid(self, cursor: Any) -> Optional[int]:
        """Extract `lastrowid` from DB-API cursor when available."""

        return getattr(cursor, "lastrowid", None)


class SQLiteDialect(Dialect):
    """SQLite dialect (`:name` parameters, supports `RETURNING`)."""

    name = "sqlite"
    paramstyle = "named"
    quote_char = '"'
    supports_returning = True

    def auto_pk_sql(self, pk_name: str) -> str:
        return f"{self.q(pk_name)} INTEGER PRIMARY KEY"


class PostgresDialect(Dialect):
    """PostgreSQL dialect (`%s` positional parameters)."""

    name = "postgres"
    paramstyle = "format"
    quote_char = '"'
    supports_returning = True

    def auto_pk_sql(self, pk_name: str) -> str:
        return f"{self.q(pk_name)} SERIAL PRIMARY KEY"


class MySQLDialect(Dialect):
    """MySQL dialect (`%s` positional parameters, no `RETURNING`)."""

    name = "mysql"
    paramstyle = "format"
    quote_char = "`"
    supports_returning = False

    def auto_pk_sql(self, pk_name: str) -> str:
        return f"{self.q(pk_name)} INT AUTO_INCREMENT PRIMARY KEY"
