from __future__ import annotations
import logging, sys, urllib, pandas as pd
from typing import Dict, List

from sqlalchemy import create_engine, inspect, text

from application.table_config import TABLE_CONFIG
from application.pipeline import Pipeline, Step
from application.use_cases.extract_use_case import ExtractUseCase
from application.use_cases.transform_use_case import TransformUseCase
from application.use_cases.load_use_case     import LoadUseCase

from infrastructure.config import Config
from infrastructure.sql_gateway  import SQLServerGateway
from infrastructure.pg_gateway   import PostgresAdminGateway
from infrastructure.pg_utils     import table_exists, create_table_with_pk, upsert_dataframe

# ─────────────── Logging ───────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("etl")

# ─────────────── Tablas a procesar ─────────────────────────────────────────
TABLES: List[str] = ["auxhor", 'obrlba', 'obrlbatar', 'obrpas', 'obrper']          #  [] → todas
if not TABLES:
    TABLES = list(TABLE_CONFIG.keys())

# ─────────────── Engines (lectura / escritura) ─────────────────────────────
sql_params = urllib.parse.quote_plus(
    f"DRIVER={{{Config.SQL_DRIVER}}};SERVER={Config.SQL_SERVER};"
    f"DATABASE={Config.SQL_DATABASE};Trusted_Connection=yes;"
)
sql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={sql_params}", future=True)
pg_engine  = create_engine(
    f"postgresql+psycopg2://{Config.PG_USER}:{Config.PG_PASSWORD}"
    f"@{Config.PG_SERVER}:{Config.PG_PORT}/{Config.PG_DATABASE}",
    future=True
)
pg_inspector = inspect(pg_engine)
CHUNK = 50_000

# ─────────────── Paso 1: diff de hashes ────────────────────────────────────
def step_diff_hashes(ctx: Dict) -> Dict:
    pending: Dict[str, List[int]] = {}
    with sql_engine.connect() as conn:
        for k in ctx["tables"]:
            cfg = TABLE_CONFIG[k]
            src, dst, pk = cfg["source_table"], cfg["target_table"], cfg["primary_key"]

            hash_src = pd.read_sql(
                f"SELECT {pk}, CAST(CHECKSUM(*) AS bigint) AS hash_crc32 FROM {src}", conn
            )

            if table_exists(pg_inspector, dst):
                hash_dst = pd.read_sql(text(f'SELECT "{pk}", hash_crc32 FROM "{dst}"'),
                                        pg_engine)
            else:
                hash_dst = pd.DataFrame(columns=[pk, "hash_crc32"])

            merged = hash_src.merge(hash_dst, on=pk, how="left",
                                    suffixes=("_src", "_dst"))
            ids = merged.loc[
                merged["hash_crc32_dst"].isna()
                | (merged["hash_crc32_src"] != merged["hash_crc32_dst"])
            ][pk].tolist()

            if ids:
                pending[k] = ids
                log.info("Tabla %s → %s filas pendientes.", k, len(ids))

    return {"pending_ids": pending}

# ─────────────── Paso 2: procesar chunks + transformación ──────────────────
def step_process_chunks(ctx: Dict) -> Dict:
    extract_uc   : ExtractUseCase   = ctx["extract_uc"]
    transform_uc : TransformUseCase = ctx["transform_uc"]
    load_uc      : LoadUseCase      = ctx["load_uc"]

    with sql_engine.connect() as conn:
        for k, ids in ctx["pending_ids"].items():
            cfg = TABLE_CONFIG[k]
            src, pk = cfg["source_table"], cfg["primary_key"]

            for i in range(0, len(ids), CHUNK):
                chunk_ids = ids[i:i+CHUNK]
                id_sql = ",".join(map(str, chunk_ids))

                df = pd.read_sql(
                    f"SELECT *, CAST(CHECKSUM(*) AS bigint) AS hash_crc32 "
                    f"FROM {src} WHERE {pk} IN ({id_sql})",
                    conn,
                )
                df = transform_uc(k, df)
                load_uc(k, df)

    return {}

# ─────────────── Construcción Pipeline ─────────────────────────────────────
def build_pipeline():
    extract_uc   = ExtractUseCase(sql_engine)
    transform_uc = TransformUseCase(extract_uc)
    load_uc      = LoadUseCase(pg_engine)

    ctx0 = {
        "tables"      : TABLES,
        "extract_uc"  : extract_uc,
        "transform_uc": transform_uc,
        "load_uc"     : load_uc,
    }

    steps = [
        Step("Diff hashes",    step_diff_hashes),
        Step("Process chunks", step_process_chunks),
    ]
    return Pipeline(steps), ctx0

# ─────────────── Smoke-checks de conexión ──────────────────────────────────
SQLServerGateway(config=Config).test_connection()
PostgresAdminGateway(config=Config).test_connection()

# ─────────────── Ejecutar pipeline ─────────────────────────────────────────
if __name__ == "__main__":
    pipeline, context = build_pipeline()
    try:
        pipeline(context)
        log.info("🏁 ETL incremental + transformaciones finalizado OK.")
    except Exception as exc:            # pylint: disable=broad-except
        log.error("🔥 Error ETL: %s", exc, exc_info=True)
        sys.exit(1)
