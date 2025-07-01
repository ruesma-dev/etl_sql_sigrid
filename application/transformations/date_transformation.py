# application/transformations/date_transformation.py
import pandas as pd
import logging
from application.transformations.base_transformation import BaseTransformation

class DateTransformation(BaseTransformation):
    def __init__(self, columns: list, null_if_zero: bool = True):
        self.columns = columns
        self.null_if_zero = null_if_zero

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.columns:
            if self.null_if_zero:
                df[col] = df[col].replace(0, pd.NA)
            df[col] = pd.to_datetime(df[col].astype(str), format="%Y%m%d", errors="coerce")
        logging.info("Convertidas columnas fecha: %s", self.columns)
        return df
