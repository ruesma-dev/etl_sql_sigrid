from __future__ import annotations

import gc
import logging
import sys
import urllib
from typing import Dict, List

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection

from application.pipeline import Pipeline, Step
from application.use_cases.extract_use_case import ExtractUseCase
from application.use_cases.transform_use_case import TransformUseCase
from application.use_cases.load_use_case import LoadUseCase
from application.table_config import TABLE_CONFIG
from helpers.stream_utils import iter_src_hashes, fetch_rows_by_ids   # ← NUEVO
from infrastructure.config import Config
from infrastructure.pg_gateway import PostgresAdminGateway
from infrastructure.pg_utils import table_exists
# (si usas upsert_dataframe / create_table_with_pk, mantenlos)
# from infrastructure.pg_utils import create_table_with_pk, upsert_dataframe

# ───── logging a fichero + consola ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("etl")

# ───── Tablas a procesar (origen SQL Server) ────────────────────────────────
TABLES: list[str] = [
    "auxhor", "obrlba", "obrlbatar", "obrpas", "obrper",
    "res", "age", "emp", "conext", "defext",
    "obrfasamb", "hmo", "hmores", "obrfas", "auxobramb",
    "obrparpre", "tar", "dcf", "dcfpro",
    "cli", "pro", "cob", "dvf", "dvfpro",
    "obr", "obrctr", "obrparpar", "cen", "con",
    "auxobrtip", "auxobrcla", "conest",
    "dca", "ctr", "dcapro", "dcaproana", "dcaprodes",
    "dcapropar", "dcaproser", "dcarec", "cer", "cerpro",
]
if not TABLES:
    TABLES = list(TABLE_CONFIG.keys())

log.info("Tablas solicitadas: %s", TABLES)

# ───── Constantes de tamaño de lote ─────────────────────────────────────────
HASH_CHUNK = 50_000      # filas por lote al comparar hashes
ROWS_CHUNK = 10_000      # filas por lote al extraer y upsertar

# ═══════════════════════════════════════════════════════════════════════════
#  PASOS DEL PIPELINE
# ═══════════════════════════════════════════════════════════════════════════
def step_init_connections(ctx: Dict) -> Dict:
    """
    1. Valida credenciales / driver ODBC.
    2. Abre conexión SQL Server (conn + engine).
    3. Prepara engine + inspector PostgreSQL.
    4. Instancia los 3 Use-Cases.
    """
    # –– test SQL Server (autenticación integrada) ––
    from infrastructure.sql_gateway import SQLServerGateway
    SQLServerGateway(config=Config).test_connection()

    # –– test / crea BD Postgres ––
    pg_admin = PostgresAdminGateway(config=Config)
    if not pg_admin.database_exists():
        pg_admin.create_database()
    pg_admin.test_connection()

    # –– engines ––
    sql_params = urllib.parse.quote_plus(
        f"DRIVER={{{Config.SQL_DRIVER}}};"
        f"SERVER={Config.SQL_SERVER};"
        f"DATABASE={Config.SQL_DATABASE};"
        "Trusted_Connection=yes;"
        "MARS_Connection=Yes;"  # ← permite varios cursores simultáneos
    )
    sql_url = f"mssql+pyodbc:///?odbc_connect={sql_params}"
    pg_url = (
        f"postgresql+psycopg2://{Config.PG_USER}:{Config.PG_PASSWORD}"
        f"@{Config.PG_SERVER}:{Config.PG_PORT}/{Config.PG_DATABASE}"
    )

    ctx["sql_engine"] = create_engine(sql_url, future=True)
    ctx["pg_engine"] = create_engine(pg_url, future=True)
    ctx["pg_inspector"] = inspect(ctx["pg_engine"])
    ctx["sql_conn"]: Connection = ctx["sql_engine"].connect()

    # –– Use-Cases ––
    ctx["extract_uc"] = ExtractUseCase(ctx["sql_conn"])
    ctx["transform_uc"] = TransformUseCase(ctx["extract_uc"])
    ctx["load_uc"] = LoadUseCase(ctx["pg_engine"])

    ctx["tables"] = TABLES
    return ctx


# ────────────────────────────────────────────────────────────────────────────
def step_streaming_etl(ctx: Dict) -> Dict:
    """
    Recorre cada tabla:
      • Stream de hashes origen (por lotes).
      • Detecta diferencias frente a destino.
      • Para los IDs pendientes:
            – extrae filas completas (fetch_rows_by_ids)
            – transforma
            – upserta
    Mantiene memoria baja y reaprovecha la misma conexión abierta.
    """
    conn: Connection = ctx["sql_conn"]
    pg_engine = ctx["pg_engine"]
    pg_inspector = ctx["pg_inspector"]
    transform_uc: TransformUseCase = ctx["transform_uc"]
    load_uc: LoadUseCase = ctx["load_uc"]
    tables = ctx["tables"]

    for key in tables:
        cfg = TABLE_CONFIG.get(key)
        if not cfg:
            log.warning("No hay configuración para %s – omitida.", key)
            continue

        src, dst, pk = cfg["source_table"], cfg["target_table"], cfg["primary_key"]
        log.info("▶ Procesando %s → %s", src, dst)

        # 1) Diccionario de hashes en destino {pk: hash_crc32}
        if table_exists(pg_inspector, dst):
            dst_hash_df = pd.read_sql(
                text(f'SELECT "{pk}", hash_crc32 FROM "{dst}"'), pg_engine
            )
            dst_map = dict(
                zip(dst_hash_df[pk].astype(np.int64), dst_hash_df.hash_crc32)
            )
        else:
            dst_map = {}

        total_upserts = 0

        # 2) Stream de hashes origen (ya usa HASHBYTES y evita LOB)
        for hash_chunk in iter_src_hashes(conn, src, pk, HASH_CHUNK):
            # ids cuyo hash es distinto o inexistente en destino
            needs_load = hash_chunk.loc[
                hash_chunk.hash_crc32.ne(hash_chunk[pk].map(dst_map).fillna(-1))
            ][pk].tolist()

            if not needs_load:
                continue

            # 3) Para esos ids: lectura real, transformación y upsert
            for i in range(0, len(needs_load), ROWS_CHUNK):
                sub_ids = needs_load[i : i + ROWS_CHUNK]

                df = fetch_rows_by_ids(conn, src, pk, sub_ids)  # ← NUEVO
                df = transform_uc(key, df)
                load_uc(key, df)            # upsert (o bulk-insert) en PG
                total_upserts += len(df)

                del df
                gc.collect()

        log.info("✓ %s upsertadas en %s.", total_upserts, dst)

    return ctx


# ────────────────────────────────────────────────────────────────────────────
def step_close_connections(ctx: Dict) -> Dict:
    """Cierra las conexiones abiertas con elegancia."""
    ctx["sql_conn"].close()
    ctx["pg_engine"].dispose()
    ctx["sql_engine"].dispose()
    log.info("Conexiones cerradas.")
    return ctx


# ═══════════════════════════════════════════════════════════════════════════
#  EJECUCIÓN
# ═══════════════════════════════════════════════════════════════════════════
pipeline = Pipeline(
    Step(step_init_connections, "init"),
    Step(step_streaming_etl, "stream"),
    Step(step_close_connections, "close"),
)

context: Dict = {}
try:
    pipeline(context)
    log.info("🏁 ETL incremental + transformaciones finalizado OK.")
except Exception as exc:  # pylint: disable=broad-except
    log.error("🔥 Error ETL: %s", exc, exc_info=True)
    sys.exit(1)
