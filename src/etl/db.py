from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert


def build_engine(pg_dsn: str):
    return create_engine(pg_dsn, pool_pre_ping=True, future=True)

class PostgresRepository:
    """
    Repo genérico para upserts en Postgres.
    Requiere que el esquema ya exista (Liquibase).
    """

    def __init__(self, engine, schema: str | None = None):
        self.engine = engine
        self.schema = schema  # None => public
        self.md = MetaData(schema=schema)
        if schema:
            self.md.reflect(bind=engine, schema=schema)
        else:
            self.md.reflect(bind=engine)
        self.tables = self.md.tables

    def _table_key(self, table_name: str) -> str:
        return f"{self.schema}.{table_name}" if self.schema else table_name

    def upsert(self, table_name: str, rows: list[dict], pk_cols: list[str]) -> None:
        if not rows:
            return

        table = self.tables[self._table_key(table_name)]
        stmt = insert(table).values(rows)

        update_cols = {
            col.name: getattr(stmt.excluded, col.name)
            for col in table.columns.values()
            if col.name not in pk_cols
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=pk_cols,
            set_=update_cols,
        )

        with self.engine.begin() as conn:
            conn.execute(stmt)

