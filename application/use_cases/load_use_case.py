#application\use_cases\load_use_case.py
from __future__ import annotations

import io
import logging
import contextlib
import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from application.table_config import TABLE_CONFIG
from infrastructure.pg_utils import (
    table_exists,
    create_table_with_pk,
    quote_ident,
)

log = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────
#  Centinela / tokens para representar NULL
# ────────────────────────────────────────────────────────────────
_NULL_SENTINEL = ""                # campo vacío ⇒ NULL en COPY
_NULL_TOKENS   = ["\\N", r"\N", "NaT", "nat", "NaN", "nan"]

# ----------------------------------------------------------------
class LoadUseCase:
    """
    FULL-RELOAD mediante COPY:

    * Limpia caracteres problematicos y literales nulos.
    * DROP-CREATE tabla destino (con PK) y COPY masivo.
    """

    def __init__(self, pg_engine: Engine):
        self.engine    = pg_engine
        self.inspector = inspect(pg_engine)

    # ------------------------------------------------------------------
    def __call__(self, table_key: str, df: pd.DataFrame):
        cfg = TABLE_CONFIG[table_key]
        dst, pk = cfg["target_table"], cfg["primary_key"]

        # ── Sanitización ───────────────────────────────────────────────
        df = (
            df.astype(object)                 # None permitido
              .where(df.notnull(), None)      # NaN / NaT → None
        )
        df.replace(_NULL_TOKENS, None, inplace=True)

        # ── Sin columnas ⇒ omite ──────────────────────────────────────
        if df.shape[1] == 0:
            log.warning("Tabla %s omitida: DataFrame sin columnas.", dst)
            return

        # ── Sin filas ⇒ sólo estructura o TRUNCATE ────────────────────
        if df.empty:
            if not table_exists(self.inspector, dst):
                create_table_with_pk(self.engine, dst, df.head(0), pk)
                self.inspector = inspect(self.engine)
                log.info("Tabla %s creada vacía (sin datos).", dst)
            else:
                with self.engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE {quote_ident(dst)}"))
                log.info("Tabla %s truncada (sin datos nuevos).", dst)
            return

        # ── FULL-reload normal ────────────────────────────────────────
        if table_exists(self.inspector, dst):
            with self.engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {quote_ident(dst)} CASCADE"))
            self.inspector = inspect(self.engine)

        create_table_with_pk(self.engine, dst, df.head(0), pk)
        self.inspector = inspect(self.engine)

        self._copy_dataframe(dst, df)
        log.info("FULL-RELOAD en %s completado (%s filas).", dst, len(df))

    # ------------------------------------------------------------------
    def _copy_dataframe(self, table: str, df: pd.DataFrame) -> None:
        """
        Inserta el DataFrame vía COPY FROM STDIN (CSV).
        Un campo **vacío** se interpreta como NULL.
        """
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False,
                  sep=",", na_rep=_NULL_SENTINEL)
        buf.seek(0)

        raw = self.engine.raw_connection()   # ← _ConnectionFairy
        try:
            with contextlib.closing(raw.cursor()) as cur:
                copy_sql = (
                    f'COPY "{table}" FROM STDIN WITH CSV '
                    "DELIMITER ',' NULL ''"
                )
                cur.copy_expert(copy_sql, buf)
            raw.commit()
        finally:
            raw.close()
