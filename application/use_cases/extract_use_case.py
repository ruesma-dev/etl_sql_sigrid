from __future__ import annotations
import logging, pandas as pd
from sqlalchemy.engine import Engine
from typing import Dict, List

log = logging.getLogger(__name__)

class ExtractUseCase:
    """
    Extrae DataFrames desde SQL Server usando un SQLAlchemy Engine.
    """
    def __init__(self, sql_engine: Engine):
        self.engine = sql_engine

    def __call__(self, tables: List[str]) -> Dict[str, pd.DataFrame]:
        dfs: Dict[str, pd.DataFrame] = {}
        with self.engine.connect() as conn:
            for tbl in tables:
                df = pd.read_sql(f"SELECT * FROM [{tbl}]", conn)
                if df.empty:
                    log.warning("Tabla %s vacía; omitida.", tbl)
                else:
                    dfs[tbl] = df
                    log.info("Extraída %s (%s filas).", tbl, len(df))
        return dfs
