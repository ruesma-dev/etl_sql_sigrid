# main.py
from __future__ import annotations
import logging, sys, urllib, pandas as pd
from sqlalchemy import create_engine, text, inspect
from application.table_config import TABLE_CONFIG
from infrastructure.config import Config
from infrastructure.pg_utils import (
    table_exists,
    create_table_with_pk,
    upsert_dataframe,
)

# ───── logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
)
log = logging.getLogger("etl_incremental")

# ───── Tablas a procesar ────────────────────────────────────────────────────
TABLES: list[str] = ["auxhor"]        #   ← pon [] para todas
if not TABLES:
    TABLES = list(TABLE_CONFIG.keys())
log.info("Tablas a procesar: %s", TABLES)

# ───── Conexiones ───────────────────────────────────────────────────────────
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
pg_engine = create_engine(pg_url, future=True)
pg_inspector = inspect(pg_engine)

CHUNK = 50_000  # <-- tamaño lote PKs

# ───── Transformaciones básicas ────────────────────────────────────────────
def transform_df(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    if cfg.get("rename_columns"):
        df = df.rename(columns=cfg["rename_columns"])
    for col in cfg.get("date_columns", []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", format="%Y%m%d")
    return df


# ───── ETL incremental por tabla ────────────────────────────────────────────
try:
    with create_engine(sql_url).connect() as sql_conn:
        for key in TABLES:
            cfg = TABLE_CONFIG.get(key)
            if not cfg:
                log.warning("No config para %s – omitida.", key)
                continue

            src, dst, pk = cfg["source_table"], cfg["target_table"], cfg["primary_key"]
            log.info("▶ Tabla %s (origen %s → destino %s)", key, src, dst)

            # --- obtener hashes origen ----------------
            hash_src = pd.read_sql(
                f"SELECT {pk}, CAST(CHECKSUM(*) AS bigint) AS hash_crc32 FROM {src}",
                sql_conn,
            )

            # --- hashes destino ------------------------
            if table_exists(pg_inspector, dst):
                hash_dst = pd.read_sql(
                    text(f'SELECT "{pk}", hash_crc32 FROM "{dst}"'), pg_engine
                )
            else:
                hash_dst = pd.DataFrame(columns=[pk, "hash_crc32"])

            merged = hash_src.merge(
                hash_dst, on=pk, how="left", suffixes=("_src", "_dst")
            )
            ids_to_load = merged.loc[
                merged["hash_crc32_dst"].isna()
                | (merged["hash_crc32_src"] != merged["hash_crc32_dst"])
            ][pk].tolist()

            if not ids_to_load:
                log.info("   Sin cambios.")
                continue

            log.info("   %s filas nuevas/modificadas.", len(ids_to_load))

            # --- procesar en chunks --------------------
            for i in range(0, len(ids_to_load), CHUNK):
                chunk_ids = ids_to_load[i : i + CHUNK]
                id_list_sql = ",".join(map(str, chunk_ids))

                df = pd.read_sql(
                    f"SELECT *, CAST(CHECKSUM(*) AS bigint) AS hash_crc32 "
                    f"FROM {src} WHERE {pk} IN ({id_list_sql})",
                    sql_conn,
                )
                df = transform_df(df, cfg)

                # crear tabla si es la primera vez
                if not table_exists(pg_inspector, dst):
                    create_table_with_pk(pg_engine, dst, df, pk)

                upsert_dataframe(pg_engine, df, dst, pk)

        log.info("🏁 ETL incremental finalizado OK.")
except Exception as exc:                             # pylint: disable=broad-except
    log.error("🔥 Error en ETL: %s", exc, exc_info=True)
    sys.exit(1)
