# infrastructure/pg_utils.py
from __future__ import annotations
import logging, pandas as pd, numpy as np
from sqlalchemy import MetaData, Table, Column, Integer, Float, DateTime, String, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)


def table_exists(inspector, table_name: str) -> bool:
    return inspector.has_table(table_name)


# --------------------------------------------------------------------------- #
def create_table_with_pk(engine: Engine, table_name: str, df: pd.DataFrame, pk_col: str) -> None:
    """
    Crea la tabla destino añadiendo columna hash_crc32 e índice.
    """
    from sqlalchemy import Index

    meta = MetaData()
    columns = []

    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            col_type = Integer
        elif pd.api.types.is_float_dtype(dtype):
            col_type = Float
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            col_type = DateTime
        else:
            col_type = String

        kwargs = {"primary_key": True} if col == pk_col else {}
        columns.append(Column(col, col_type, **kwargs))

    # asegura la columna hash
    if "hash_crc32" not in df.columns:
        columns.append(Column("hash_crc32", Integer))

    table = Table(table_name, meta, *columns)
    meta.create_all(engine)

    # índice sobre hash para acelerar el WHERE en upsert
    with engine.begin() as conn:
        idx = Index(f"idx_{table_name}_hash", table.c.hash_crc32)
        idx.create(bind=conn)

    logger.info("Tabla %s creada con PK '%s' y columna hash_crc32.", table_name, pk_col)


# --------------------------------------------------------------------------- #
def upsert_dataframe(engine, df, dst_table_name, pk_col):
    # 1. Conexión explícita (2.x ya no permite engine.execute)
    with engine.begin() as conn:
        meta = MetaData()                # ← sin bind
        meta.reflect(bind=conn)          # refleja el esquema que ya creaste

        dst = Table(dst_table_name, meta, autoload_with=conn)

        # 2. Crea la sentencia INSERT ... ON CONFLICT
        stmt = pg_insert(dst).values(df.to_dict(orient="records"))
        update_cols = {c.name: stmt.excluded[c.name]
                       for c in dst.columns if c.name != pk_col}

        stmt = stmt.on_conflict_do_update(
            index_elements=[pk_col],
            set_=update_cols
        )

        # 3. Ejecuta
        conn.execute(stmt)
