# application/transformations/rename_columns_transformation.py
import pandas as pd
import logging
from application.transformations.base_transformation import BaseTransformation

class RenameColumnsTransformation(BaseTransformation):
    def __init__(self, mapping: dict):
        self.mapping = mapping

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns=self.mapping)
        logging.info("Renombradas columnas: %s", self.mapping)
        return df
