# infrastructure/pg_utils.py
from __future__ import annotations

import logging
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Float,
    DateTime,
    String,
    Index,
    inspect,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────
def table_exists(inspector, table_name: str) -> bool:
    """
    Devuelve True si la tabla existe en PostgreSQL.
    `inspector` es una instancia de sqlalchemy.inspect(engine)
    """
    return inspector.has_table(table_name)


# ────────────────────────────────────────────────────────────────────────────
def create_table_with_pk(
    engine: Engine, table_name: str, df: pd.DataFrame, pk_col: str
) -> None:
    """
    Crea la tabla `table_name` con todas las columnas del DataFrame,
    establece `pk_col` como PRIMARY KEY y añade la columna `hash_crc32`
    con un índice auxiliar.
    """
    meta = MetaData()
    columns: List[Column] = []

    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            col_type = Integer
        elif pd.api.types.is_float_dtype(dtype):
            col_type = Float
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            col_type = DateTime
        else:
            col_type = String

        kw = {"primary_key": True} if col == pk_col else {}
        columns.append(Column(col, col_type, **kw))

    # asegúrate de que existe la columna del hash
    if "hash_crc32" not in df.columns:
        columns.append(Column("hash_crc32", Integer))

    table = Table(table_name, meta, *columns)
    # solo crea si no existe
    meta.create_all(engine, checkfirst=True)

    with engine.begin() as conn:
        idx = Index(f"idx_{table_name}_hash", table.c.hash_crc32)
        idx.create(bind=conn, checkfirst=True)

    logger.info(
        "Tabla %s creada con PK '%s', columna hash_crc32 e índice auxiliar.",
        table_name,
        pk_col,
    )


# ────────────────────────────────────────────────────────────────────────────
def _pg_type_from_series(series: pd.Series):
    """Convierte un dtype pandas a un tipo SQLAlchemy simple."""
    if pd.api.types.is_integer_dtype(series):
        return Integer
    if pd.api.types.is_float_dtype(series):
        return Float
    if pd.api.types.is_datetime64_any_dtype(series):
        return DateTime
    return String  # fallback genérico


# ────────────────────────────────────────────────────────────────────────────
def upsert_dataframe(engine: Engine, df: pd.DataFrame, dst_table_name: str, pk_col: str):
    """
    • Añade dinámicamente cualquier columna faltante a `dst_table_name`.
    • Ejecuta un INSERT ... ON CONFLICT (UPSERT) según `pk_col`.

    Esto permite procesar la tabla por chunks sin fallar si
    aparecen columnas nuevas en un chunk posterior.
    """
    with engine.begin() as conn:
        meta = MetaData()
        meta.reflect(bind=conn)

        dst = Table(dst_table_name, meta, autoload_with=conn)
        existing_cols = {c.name for c in dst.columns}

        # ── detectar y crear columnas que falten ───────────────────
        missing = [c for c in df.columns if c not in existing_cols]
        for col in missing:
            col_type = _pg_type_from_series(df[col])
            logger.info(
                "Añadiendo columna '%s' (%s) a %s",
                col,
                col_type.__name__,
                dst_table_name,
            )
            conn.execute(
                text(
                    f'ALTER TABLE "{dst_table_name}" '
                    f'ADD COLUMN "{col}" {col_type().compile(dialect=conn.dialect)}'
                )
            )

        if missing:
            # refrescamos metadata para que refleje las nuevas columnas
            meta = MetaData()
            meta.reflect(bind=conn)
            dst = Table(dst_table_name, meta, autoload_with=conn)

        # ── construir INSERT … ON CONFLICT ─────────────────────────
        stmt = pg_insert(dst).values(df.to_dict(orient="records"))
        update_cols = {
            c.name: stmt.excluded[c.name] for c in dst.columns if c.name != pk_col
        }

        conn.execute(
            stmt.on_conflict_do_update(index_elements=[pk_col], set_=update_cols)
        )
        logger.info("UPSERT en %s completado (%s filas).", dst_table_name, len(df))
