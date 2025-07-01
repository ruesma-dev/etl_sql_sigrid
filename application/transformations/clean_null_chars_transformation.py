# application/transformations/clean_null_chars_transformation.py
from __future__ import annotations

import logging
from typing import Any, Iterable

import pandas as pd

log = logging.getLogger(__name__)

_NULL_CHAR = "\x00"


# ────────────────────────────────────────────────────────────────────────────
def _decode_and_clean(val: Any) -> Any:
    """
    • Bytes/bytearray/memoryview → str (UTF-8, fallback latin-1).
    • Si es str, elimina el carácter NUL.
    • Devuelve el valor sin modificar en cualquier otro caso.
    """
    if val is None:
        return val

    if isinstance(val, (bytes, bytearray, memoryview)):
        for enc in ("utf-8", "latin-1"):
            try:
                val = val.decode(enc, errors="replace")
                break
            except Exception:  # pragma: no cover
                continue

    if isinstance(val, str):
        return val.replace(_NULL_CHAR, "")

    return val


# ────────────────────────────────────────────────────────────────────────────
class CleanNullCharsTransformation:
    """
    Elimina caracteres \\x00 y convierte bytes a texto en las columnas indicadas
    (o en todas si `columns` es None).
    """

    def __init__(self, columns: Iterable[str] | None = None):
        self.columns = list(columns) if columns is not None else None

    # ---------------------------------------------------------------------#
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = self.columns or df.columns

        for col in cols:
            if col not in df.columns:
                continue

            # map() evita el uso del accesor .str, tolera None/NA y es seguro
            df[col] = df[col].map(_decode_and_clean)

        return df
