from __future__ import annotations

import pandas as pd
from pandas.api.types import is_string_dtype

# ---------------------------------------------------------------------------
# Compatibilidad con tu jerarquía de transformaciones
# ---------------------------------------------------------------------------
try:
    # Tu proyecto define la clase BaseTransformation
    from application.transformations.base_transformation import (
        BaseTransformation as Transformation,
    )
except ImportError:
    # Fallback en caso de que no exista
    class Transformation:  # type: ignore
        def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...


class CleanNullCharsTransformation(Transformation):
    """
    • Elimina caracteres NUL (“\\x00”) que a veces llegan en campos texto.
    • Decodifica columnas con bytes/bytearray → str para poder aplicar `.str`.
    • Convierte literales que representan nulos (`\\N`, `NaN`, `NaT`, …) en
      valores nulos reales (`pd.NA`) antes de la carga.
    """

    # Literales que deben tratarse como NULL
    NULL_TOKENS = ["NaT", "nat", "NaN", "nan", r"\N", r"\\N"]

    # ------------------------------------------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            series = df[col]

            # 1) Si la serie contiene bytes → str (UTF-8, ignorando errores)
            if (
                series.dtype == object
                and series.apply(lambda v: isinstance(v, (bytes, bytearray))).any()
            ):
                df[col] = series.apply(
                    lambda v: v.decode("utf-8", "ignore")
                    if isinstance(v, (bytes, bytearray))
                    else v
                )
                series = df[col]  # refrescamos referencia

            # 2) Limpiar caracteres NUL en columnas texto
            if is_string_dtype(series) or series.dtype == object:
                mask = series.notna()
                if mask.any():
                    df.loc[mask, col] = (
                        series.loc[mask]
                        .astype(str)
                        .str.replace("\x00", "", regex=False)
                    )

        # 3) Sustituir tokens literales por nulos reales
        df.replace(to_replace=self.NULL_TOKENS, value=pd.NA, inplace=True, regex=False)

        return df
