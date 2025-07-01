# helpers/stream_utils.py
"""
Iteradores para leer (PK, hash) y los registros completos por chunks.
Se usa una segunda conexión para evitar que el cursor de pandas
mantenga ocupada la principal mientras seguimos iterando.
"""
from __future__ import annotations

from typing import Iterator, List

import pandas as pd
from sqlalchemy.engine import Connection, Engine

from helpers.checksum_utils import build_checksum_query


# ──────────────────────────────────────────────────────────
def iter_src_hashes(
    conn: Connection,
    table: str,
    pk: str,
    chunk: int = 50_000,
) -> Iterator[pd.DataFrame]:
    """
    Devuelve DataFrames [pk, hash_crc32] usando el *mismo* `conn`.
    Mientras el cursor siga abierto no puede ejecutarse otro comando en
    esa conexión → por eso, en las lecturas puntuales abriremos otra.
    """
    sql = build_checksum_query(conn, table, pk)
    return pd.read_sql_query(sql, conn, chunksize=chunk)


# ──────────────────────────────────────────────────────────
def fetch_rows_by_ids(
    conn: Connection,
    table: str,
    id_col: str,
    ids: List[int],
) -> pd.DataFrame:
    """
    Lee las filas completas (+hash) de los IDs indicados **abriendo una
    conexión nueva del mismo pool** para evitar el error HY000.
    """
    engine: Engine = conn.engine           # reutilizamos el pool
    with engine.connect() as tmp_conn:     # ← conexión independiente
        checksum_sql = build_checksum_query(tmp_conn, table, id_col)
        id_list_sql = ",".join(map(str, ids))

        sql = f"""
        WITH src AS (
            SELECT s.*, h.hash_crc32
            FROM {table} AS s
            JOIN ({checksum_sql}) AS h
              ON s.[{id_col}] = h.[{id_col}]
        )
        SELECT * FROM src WHERE [{id_col}] IN ({id_list_sql})
        """
        return pd.read_sql(sql, tmp_conn)
