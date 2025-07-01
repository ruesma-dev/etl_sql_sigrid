# helpers/checksum_utils.py
"""
Construye la sentencia SQL para obtener (PK, hash_crc32) de una tabla
SQL Server SIN usar CHECKSUM(*), que falla cuando hay columnas LOB
(text, ntext, image, varbinary(max)…).

Estrategia
──────────
1. Consultamos sys.columns para conocer el tipo de cada columna.
2. Filtramos las LOB problemáticas (text, ntext, image, xml, varbinary…).
   • Si aún las necesitas para detectar cambios, márcalas en
     ALWAYS_INCLUDE_LOB.
3. Generamos un HASHBYTES('SHA2_256', col1 + '|' + col2 + …)     → varbinary
4. CAST del hash a BIGINT mediante SUBSTRING (suficiente para detectar
   cambios; no pretende evitar colisiones criptográficas).

Este hash es sólo para dif-detección, no para seguridad.
"""

from __future__ import annotations

from typing import List
from sqlalchemy.engine import Connection
from sqlalchemy import text

# Cambia a True si deseas incluir TODO aunque sea LOB
ALWAYS_INCLUDE_LOB: bool = False

# ---------------------------------------------------------------------------
def _non_lob_columns(conn: Connection, table: str) -> List[str]:
    """Devuelve las columnas que NO son LOB problemáticas."""
    lob_types = {
        "text",
        "ntext",
        "image",
        "xml",
        "hierarchyid",
        "sql_variant",
        "geography",
        "geometry",
        "varbinary",
    }

    sql = """
    SELECT name, system_type_name
    FROM sys.dm_exec_describe_first_result_set
         ('SELECT * FROM ' + QUOTENAME(:tbl), NULL, 0)
    """
    rows = conn.execute(text(sql), {"tbl": table}).fetchall()
    cols: List[str] = []

    for col_name, type_name in rows:
        t = type_name.lower().split("(")[0]  # quitamos (max) / (50)…
        if t not in lob_types or ALWAYS_INCLUDE_LOB:
            cols.append(col_name)

    return cols


# ---------------------------------------------------------------------------
def build_checksum_query(conn: Connection, table: str, pk: str) -> str:
    """
    Produce la sentencia SELECT <pk>, hash_crc32 … lista para usar con
    `pd.read_sql_query`.
    """
    cols = _non_lob_columns(conn, table)
    if pk not in cols:
        cols.insert(0, pk)  # aseguramos que la PK está incluida

    # Concatenamos con '|' para reducir colisiones                ↓↓↓
    concat_expr = " + '|' + ".join(f"COALESCE(CONVERT(NVARCHAR(MAX), [{c}]), '')" for c in cols)

    sql = f"""
    SELECT
        [{pk}],
        -- SHA2_256 da 32 bytes. Tomamos los 8 primeros como BIGINT.
        CAST(
            CONVERT(BIGINT,
                CONVERT(VARBINARY(8),
                    HASHBYTES('SHA2_256', {concat_expr})
                )
            ) AS BIGINT
        ) AS hash_crc32
    FROM {table}
    """
    return sql
