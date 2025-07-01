from __future__ import annotations
import pandas as pd, logging
from sqlalchemy.engine import Engine
from application.table_config import TABLE_CONFIG
from infrastructure.pg_utils import table_exists, create_table_with_pk, upsert_dataframe
from sqlalchemy import inspect

log = logging.getLogger(__name__)

class LoadUseCase:
    """
    Inserta / upserta DataFrames en PostgreSQL usando Engine.
    """
    def __init__(self, pg_engine: Engine):
        self.pg_engine   = pg_engine
        self.pg_inspector= inspect(pg_engine)

    def __call__(self, table_key: str, df: pd.DataFrame):
        cfg = TABLE_CONFIG[table_key]
        dst, pk = cfg["target_table"], cfg["primary_key"]

        if not table_exists(self.pg_inspector, dst):
            create_table_with_pk(self.pg_engine, dst, df, pk)

        upsert_dataframe(self.pg_engine, df, dst, pk)
        log.info("UPSERT en %s completado.", dst)
