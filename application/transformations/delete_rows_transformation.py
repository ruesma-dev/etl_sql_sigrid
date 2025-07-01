# application/transformations/delete_rows_transformation.py
import pandas as pd
import logging
from application.transformations.base_transformation import BaseTransformation

class DeleteRowsTransformation(BaseTransformation):
    def __init__(self, criteria: list):
        self.criteria = criteria

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        initial = len(df)
        for cond in self.criteria:
            for col, val in cond.items():
                df = df[df[col] != val]
        logging.info("Eliminadas %s filas.", initial - len(df))
        return df
