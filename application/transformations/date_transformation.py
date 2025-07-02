# application/transformations/date_transformation.py
from __future__ import annotations

import logging
import pandas as pd

log = logging.getLogger(__name__)


class DateTransformation:
    """
    Normaliza columnas de fecha.

    • Reemplaza 0 / 1900-01-01 por NA
    • Convierte a datetime       → nueva col (o la misma)
    • Omite con elegancia DF vacíos o columnas ausentes
    """

    def __init__(self, date_cols: dict[str, str]):
        # mapping {col_origen: col_destino}
        self.date_cols = date_cols

    # ------------------------------------------------------------------ #
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # DataFrame sin filas → nada que hacer
        if df.empty:
            return df

        for col, new_col in self.date_cols.items():
            if col not in df.columns:
                log.debug("DateTransformation: columna %s ausente; se omite.", col)
                continue

            # Limpiamos y convertimos
            df[col] = df[col].replace(0, pd.NA)
            df[new_col] = pd.to_datetime(df[col], errors="coerce")

            # Si se pidió renombrar, quitamos la original
            if new_col != col:
                df.drop(columns=[col], inplace=True)

        return df
