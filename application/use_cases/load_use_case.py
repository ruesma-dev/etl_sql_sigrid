from __future__ import annotations

import io
import logging
from typing import Iterable

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from application.table_config import TABLE_CONFIG
from infrastructure.pg_utils import (
    table_exists,
    create_table_with_pk,
    upsert_dataframe,
)

log = logging.getLogger(__name__)


class LoadUseCase:
    """
    Inserta / upserta DataFrames en PostgreSQL.

    Parámetros
    ----------
    pg_engine : Engine
        Engine SQLAlchemy ya configurado.
    bulk : bool, default=False
        • False ⇒ upsert con `INSERT … ON CONFLICT …` (comportamiento original)
        • True  ⇒ inserción masiva (`COPY FROM STDIN`) **sin upsert**
    """

    def __init__(self, pg_engine: Engine, *, bulk: bool = False) -> None:
        self.pg_engine = pg_engine
        self.pg_inspector = inspect(pg_engine)
        self._bulk = bulk

    # ────────────────────────────────────────────────────────────────────
    def __call__(self, table_key: str, df: pd.DataFrame) -> None:
        cfg = TABLE_CONFIG[table_key]
        dst, pk = cfg["target_table"], cfg["primary_key"]

        # ─── LIMPIEZA NULOS / NaT ──────────────────────────────────────
        df = df.astype(object)                      # 1) todo object ⇒ None válido
        df = df.where(pd.notnull(df), None)         # 2) NaN / NaT / pd.NA → None
        df.replace(                                 # 3) literales 'nan', 'NaT'…
            to_replace=["NaT", "nat", "NaN", "nan"],
            value=None,
            inplace=True,
        )

        # ─── CREAR TABLA SI NO EXISTE ─────────────────────────────────
        if not table_exists(self.pg_inspector, dst):
            create_table_with_pk(self.pg_engine, dst, df, pk)
            # refrescamos inspector para que conozca la nueva tabla
            self.pg_inspector = inspect(self.pg_engine)

        # ─── INSERT / UPSERT ──────────────────────────────────────────
        if self._bulk:
            self._copy_dataframe(dst, df)
            log.info("COPY en %s completado.", dst)
        else:
            upsert_dataframe(self.pg_engine, df, dst, pk)
            log.info("UPSERT en %s completado.", dst)

    # ────────────────────────────────────────────────────────────────────
    #  MÉTODOS AUXILIARES
    # ────────────────────────────────────────────────────────────────────
    def _copy_dataframe(self, table: str, df: pd.DataFrame) -> None:
        """
        Inserta el DataFrame usando `COPY FROM STDIN` (mucho más rápido
        que executemany).  No actualiza registros existentes.
        """
        buffer = io.StringIO()
        # TSV: tab como separador, \N como NULL (entendido por PostgreSQL)
        df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
        buffer.seek(0)

        # Conexión en bruto para usar copy_from
        with self.pg_engine.raw_connection() as raw_conn:
            with raw_conn.cursor() as cur:
                cur.copy_from(
                    buffer,
                    table,              # tabla destino
                    sep="\t",
                    null="\\N",
                    columns=list(df.columns),
                )
            raw_conn.commit()
