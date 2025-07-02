from __future__ import annotations

import io
import csv
from typing import List, Iterable
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


# ────────────────────────────────────────────────────────────────────
# Helpers de IDENTIFICADORES
# ────────────────────────────────────────────────────────────────────
def quote_ident(ident: str) -> str:
    """
    Devuelve el identificador escapado con comillas dobles sin requerir
    una conexión psycopg2 (evitamos el TypeError que obtuviste).

    Reglas mínimas de PostgreSQL:
      • Se duplica cualquier comilla doble interna.
      • Se envuelve todo en comillas dobles.
    """
    return '"' + ident.replace('"', '""') + '"'


# ────────────────────────────────────────────────────────────────────
def table_exists(inspector, table: str) -> bool:
    return inspector.has_table(table)


def create_table_with_pk(engine: Engine, table: str, df, pk: str) -> None:
    cols_ddl = ", ".join(f"{quote_ident(c)} TEXT" for c in df.columns)
    ddl = f"CREATE TABLE {quote_ident(table)} ({cols_ddl}, PRIMARY KEY ({quote_ident(pk)}))"
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ────────────────────────────────────────────────────────────────────
def copy_dataframe(engine: Engine, df, table: str) -> None:
    """
    COPY FROM STDIN directamente desde un buffer en memoria, alineando
    columnas dinámicamente con cada DataFrame.
    """
    buf = io.StringIO()
    df.to_csv(
        buf,
        sep="\t",
        index=False,
        header=False,
        na_rep="\\N",
        quoting=csv.QUOTE_NONE,
        escapechar="\\",
    )
    buf.seek(0)

    raw = engine.raw_connection()          # conexión psycopg2 subyacente
    try:
        cur = raw.cursor()
        cur.copy_from(buf, table, sep="\t", null="\\N", columns=list(df.columns))
        raw.commit()
        cur.close()
    finally:
        raw.close()


# ────────────────────────────────────────────────────────────────────
def merge_from_staging(engine: Engine, stg: str, dst: str, pk: str) -> None:
    """
    Inserta / actualiza sólo las columnas presentes en la staging.
    """
    insp = inspect(engine)
    stg_cols = {c["name"] for c in insp.get_columns(stg)}
    dst_cols = {c["name"] for c in insp.get_columns(dst)}
    common   = [c for c in stg_cols & dst_cols]

    if pk not in common:
        raise ValueError(f"La clave primaria «{pk}» no está en {stg}")

    cols_csv   = ", ".join(quote_ident(c) for c in common)
    select_csv = ", ".join(f's.{quote_ident(c)}' for c in common)
    update_csv = ", ".join(f'{quote_ident(c)} = EXCLUDED.{quote_ident(c)}'
                           for c in common if c != pk)

    sql = f"""
    INSERT INTO {quote_ident(dst)} ({cols_csv})
    SELECT {select_csv}
    FROM {quote_ident(stg)} AS s
    ON CONFLICT ({quote_ident(pk)}) DO UPDATE
    SET {update_csv};
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
