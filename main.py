# main.py
from __future__ import annotations

import gc
import logging
import sys
import urllib

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection

from application.pipeline import Pipeline, Step
from application.use_cases.extract_use_case import ExtractUseCase   # sigue igual
from application.use_cases.transform_use_case import TransformUseCase
from application.use_cases.load_use_case import LoadUseCase          #  ←  nuevo
from application.table_config import TABLE_CONFIG
from infrastructure.config import Config
from infrastructure.pg_gateway import PostgresAdminGateway
from infrastructure.sql_gateway import SQLServerGateway

# ───── logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("etl.log", mode="a", encoding="utf-8")],
)
log = logging.getLogger("etl")

# ───── Tablas a procesar ────────────────────────────────────────────────────
TABLES: list[str] = [
    "auxhor", "obrlba", "obrlbatar", "obrpas", "obrper",
    "res", "age", "emp", "conext", "defext",
    "obrfasamb", "hmo", "hmores", "obrfas", "auxobramb",
    "obrparpre",
    "tar", "dcf", "dcfpro",
    "cli",
    "pro"
    , "cob", "dvf", "dvfpro",
    "obr", "obrctr", "obrparpar", "cen", "con",
    "auxobrtip", "auxobrcla", "conest",
    "dca", "ctr", "dcapro", "dcaproana", "dcaprodes",
    "dcapropar", "dcaproser", "dcarec", "cer", "cerpro",
]

if not TABLES:
    TABLES = list(TABLE_CONFIG.keys())

# ═══════════════════════════════════════════════════════════════════════════
#  PASOS DEL PIPELINE
# ═══════════════════════════════════════════════════════════════════════════
def step_init_connections(ctx: dict) -> dict:
    """Comprueba credenciales y abre motores SQL Server / Postgres."""
    SQLServerGateway(config=Config).test_connection()

    pg_admin = PostgresAdminGateway(config=Config)
    if not pg_admin.database_exists():
        pg_admin.create_database()
    pg_admin.test_connection()

    # engines ----------------------------------------------------------------
    sql_params = urllib.parse.quote_plus(
        f"DRIVER={{{Config.SQL_DRIVER}}};"
        f"SERVER={Config.SQL_SERVER};"
        f"DATABASE={Config.SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )
    sql_url = f"mssql+pyodbc:///?odbc_connect={sql_params}"
    pg_url = (
        f"postgresql+psycopg2://{Config.PG_USER}:{Config.PG_PASSWORD}"
        f"@{Config.PG_SERVER}:{Config.PG_PORT}/{Config.PG_DATABASE}"
    )

    ctx["sql_engine"] = create_engine(sql_url, future=True)
    ctx["pg_engine"] = create_engine(pg_url, future=True)
    ctx["sql_conn"]: Connection = ctx["sql_engine"].connect()

    # Use-cases --------------------------------------------------------------
    ctx["extract_uc"]   = ExtractUseCase(ctx["sql_conn"])       # sin cambios internos
    ctx["transform_uc"] = TransformUseCase(ctx["extract_uc"])
    ctx["load_uc"]      = LoadUseCase(ctx["pg_engine"])

    ctx["tables"] = TABLES
    return ctx


# ────────────────────────────────────────────────────────────────────────────
def step_full_reload_etl(ctx: dict) -> None:
    """
    Nueva estrategia:
    1. Extrae la tabla completa de SQL Server.
    2. Transforma DataFrame.
    3. **Siempre** borra destino y hace COPY masivo completo.
    """
    conn: Connection          = ctx["sql_conn"]
    transform_uc: TransformUseCase = ctx["transform_uc"]
    load_uc: LoadUseCase      = ctx["load_uc"]
    tables                    = ctx["tables"]

    for key in tables:
        cfg = TABLE_CONFIG.get(key)
        if not cfg:
            log.warning("Sin configuración para %s – omitida.", key)
            continue

        src = cfg["source_table"]
        log.info("▶ Extrayendo %s (FULL) …", src)

        df = pd.read_sql_query(f"SELECT * FROM {src}", conn)
        log.info("   %s filas extraídas.", len(df))

        df = transform_uc(key, df)
        load_uc(key, df)                   # ← ahora es FULL RELOAD

        del df
        gc.collect()

    return ctx


# ────────────────────────────────────────────────────────────────────────────
def step_close_connections(ctx: dict) -> None:
    ctx["sql_conn"].close()
    ctx["pg_engine"].dispose()
    ctx["sql_engine"].dispose()
    log.info("Conexiones cerradas.")
    return ctx


# ═══════════════════════════════════════════════════════════════════════════
pipeline = Pipeline(
    Step(step_init_connections, "init"),
    Step(step_full_reload_etl,   "reload"),      # ← nombre nuevo
    Step(step_close_connections, "close"),
)

if __name__ == "__main__":
    try:
        pipeline({})
        log.info("🏁 ETL full-reload finalizado OK.")
    except Exception as exc:  # pylint: disable=broad-except
        log.error("🔥 Error ETL: %s", exc, exc_info=True)
        sys.exit(1)
