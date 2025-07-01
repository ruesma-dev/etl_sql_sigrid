# application/transformations/drop_null_or_zero_columns_transformation.py
import pandas as pd
import logging
from application.transformations.base_transformation import BaseTransformation

class DropNullOrZeroColumnsTransformation(BaseTransformation):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        drop = []
        for col in df.columns:
            s = df[col]
            if s.isnull().all():
                drop.append(col)
            elif pd.api.types.is_numeric_dtype(s) and (s.dropna() == 0).all():
                drop.append(col)
            elif pd.api.types.is_string_dtype(s) and (s.dropna() == "").all():
                drop.append(col)
        if drop:
            df = df.drop(columns=drop)
            logging.info("Columnas eliminadas: %s", drop)
        return df
