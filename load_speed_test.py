# tests/load_speed_test.py
from __future__ import annotations

import time, logging, urllib, pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from infrastructure.config import Config
from infrastructure.pg_utils import (
    create_table_with_pk,
    copy_dataframe,
    quote_ident,
)
from application.table_config import TABLE_CONFIG
from application.use_cases.extract_use_case import ExtractUseCase   # (solo para coherencia)
from application.use_cases.load_use_case import LoadUseCase


# ────────────────────────────────────────────────────────────────────────────
#  LISTA DE TABLAS A MEDIR  ←⇠  EDITA AQUÍ
# ────────────────────────────────────────────────────────────────────────────
TABLE_KEYS: list[str] = [
    # "obrparpre",
    "con",
    # "age",
    # "otro_key",
]

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
)
log = logging.getLogger("load_speed_test")


# ─── Conn helpers ───────────────────────────────────────────────────────────
def _sql_engine() -> Engine:
    sql_params = urllib.parse.quote_plus(
        f"DRIVER={{{Config.SQL_DRIVER}}};"
        f"SERVER={Config.SQL_SERVER};"
        f"DATABASE={Config.SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={sql_params}", future=True)


def _pg_engine() -> Engine:
    return create_engine(
        f"postgresql+psycopg2://{Config.PG_USER}:{Config.PG_PASSWORD}"
        f"@{Config.PG_SERVER}:{Config.PG_PORT}/{Config.PG_DATABASE}",
        future=True,
    )


def _sanitize(df: pd.DataFrame) -> pd.DataFrame:
    """Iguala la limpieza que hace LoadUseCase."""
    return (
        df.astype(object)
          .where(df.notnull(), None)
          .replace(["NaT", "nat", "NaN", "nan"], None)
    )


# ─── Benchmark para una tabla ───────────────────────────────────────────────
def bench_table(table_key: str, sql_engine: Engine, pg_engine: Engine) -> tuple[float, float, int]:
    cfg          = TABLE_CONFIG[table_key]
    src_tbl      = cfg["source_table"]
    dst_tbl      = cfg["target_table"]
    pk           = cfg["primary_key"]

    # 1) Extraer todo de SQL Server
    df = pd.read_sql(f"SELECT * FROM {src_tbl}", sql_engine)
    df = _sanitize(df)
    n_rows = len(df)
    log.info("Extraídas %s filas de %s.", n_rows, src_tbl)

    # 2) FULL RELOAD  (DROP + CREATE + COPY)
    t0 = time.perf_counter()
    with pg_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {quote_ident(dst_tbl)} CASCADE"))
    create_table_with_pk(pg_engine, dst_tbl, df, pk)
    copy_dataframe(pg_engine, df, dst_tbl)
    reload_secs = time.perf_counter() - t0
    log.info("FULL-RELOAD %s: %.2f s", dst_tbl, reload_secs)

    # 3) UPSERT (staging + COPY + MERGE)
    loader = LoadUseCase(pg_engine)
    t1 = time.perf_counter()
    loader(table_key, df)
    upsert_secs = time.perf_counter() - t1
    log.info("UPSERT       %s: %.2f s", dst_tbl, upsert_secs)

    return reload_secs, upsert_secs, n_rows


# ─── Ejecución principal ────────────────────────────────────────────────────
def main() -> None:
    sql_engine = _sql_engine()
    pg_engine  = _pg_engine()

    results: list[tuple[str, int, float, float]] = []

    for key in TABLE_KEYS:
        if key not in TABLE_CONFIG:
            log.warning("Clave %s no existe en TABLE_CONFIG – omitida.", key)
            continue

        r_secs, u_secs, rows = bench_table(key, sql_engine, pg_engine)
        results.append((key, rows, r_secs, u_secs))

    # Resumen final
    print("\n┌────────────────────────────────────────────────────────────┐")
    print("│   Benchmark full-reload vs. upsert                         │")
    print("├────────┬────────────┬──────────────┬──────────────┤")
    print("│ Tabla  │ Filas      │ Full reload  │ Upsert       │")
    print("├────────┼────────────┼──────────────┼──────────────┤")
    for key, rows, r_secs, u_secs in results:
        print(
            f"│ {key:<6} │ {rows:>10,} │ {r_secs:>10.2f} s │ {u_secs:>10.2f} s │"
        )
    print("└────────┴────────────┴──────────────┴──────────────┘")


if __name__ == "__main__":
    if not TABLE_KEYS:
        log.error("La lista TABLE_KEYS está vacía. Añade al menos una clave de tabla.")
    else:
        main()
