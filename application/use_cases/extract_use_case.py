# application/use_cases/extract_use_case.py
from __future__ import annotations

from typing import List, Dict
import pandas as pd
from sqlalchemy.engine import Engine, Connection


class ExtractUseCase:
    """
    Lectura desde SQL Server.

    • Acepta tanto Engine como Connection en el constructor.
    • stream_full(table, chunk)  → generator por lotes
    • __call__(tables)           → dict con DataFrames completos
    """

    def __init__(self, sql_obj: Engine | Connection):
        # pandas.read_sql_query funciona con ambos tipos indistintamente
        self.sql_src: Engine | Connection = sql_obj

    # ------------------------------------------------------------------ #
    def stream_full(self, table: str, chunk_rows: int = 100_000):
        """Devuelve un generador de DataFrames por lotes."""
        sql = f"SELECT * FROM {table}"
        return pd.read_sql_query(sql, self.sql_src, chunksize=chunk_rows)

    # ------------------------------------------------------------------ #
    def __call__(self, tables: List[str]) -> Dict[str, pd.DataFrame]:
        """Carga por completo varias tablas pequeñas en memoria."""
        return {
            t: pd.read_sql_query(f"SELECT * FROM {t}", self.sql_src)
            for t in tables
        }
