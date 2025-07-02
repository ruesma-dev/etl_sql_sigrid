# application/use_cases/load_use_case.py
from __future__ import annotations

import logging
import io
import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from application.table_config import TABLE_CONFIG
from infrastructure.pg_utils import (
    table_exists,
    create_table_with_pk,
    copy_dataframe,   # util que hace COPY FROM STDIN
    quote_ident,
)

log = logging.getLogger(__name__)


class LoadUseCase:
    """
    Estrategia FULL-RELOAD:

    1. Si el DataFrame no tiene filas → TRUNCATE / crea estructura y termina.
    2. Si no tiene columnas (p.e. todas eliminadas) → omite tabla.
    3. Para el resto:
         • DROP TABLE destino
         • CREATE TABLE
         • COPY masivo
    """

    def __init__(self, pg_engine: Engine):
        self.engine    = pg_engine
        self.inspector = inspect(pg_engine)

    # ------------------------------------------------------------------ #
    def __call__(self, table_key: str, df: pd.DataFrame):
        cfg = TABLE_CONFIG[table_key]
        dst, pk = cfg["target_table"], cfg["primary_key"]

        # ─── Sanitizar nulos / tipos ───────────────────────────────────────
        df = (
            df.astype(object)
              .where(df.notnull(), None)
              .replace(to_replace=["NaT", "nat", "NaN", "nan"], value=None)
        )

        # ─── Caso 0 columnas: nada que crear/cargar ────────────────────────
        if df.shape[1] == 0:
            log.warning(
                "Tabla %s: DataFrame sin columnas tras transformaciones; se omite carga.",
                dst,
            )
            return

        # ─── Caso 0 filas: solo asegurar estructura ───────────────────────
        if df.empty:
            if not table_exists(self.inspector, dst):
                create_table_with_pk(self.engine, dst, df.head(0), pk)
                self.inspector = inspect(self.engine)
                log.info("Tabla %s creada vacía (sin datos).", dst)
            else:
                with self.engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE {quote_ident(dst)}"))
                log.info("Tabla %s truncada (sin datos para cargar).", dst)
            return

        # ─── FULL-RELOAD normal ───────────────────────────────────────────
        # 1. DROP existente
        if table_exists(self.inspector, dst):
            with self.engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {quote_ident(dst)} CASCADE"))
            self.inspector = inspect(self.engine)

        # 2. CREATE nueva con PK
        create_table_with_pk(self.engine, dst, df.head(0), pk)
        self.inspector = inspect(self.engine)

        # 3. COPY masivo
        copy_dataframe(self.engine, df, dst)

        log.info("FULL-RELOAD en %s completado (%s filas).", dst, len(df))
