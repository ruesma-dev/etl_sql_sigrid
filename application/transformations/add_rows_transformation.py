# application/transformations/add_rows_transformation.py
import pandas as pd
import logging
from application.transformations.base_transformation import BaseTransformation

class AddRowsTransformation(BaseTransformation):
    def __init__(self, rows_to_add: list):
        self.rows_to_add = rows_to_add

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.rows_to_add:
            df = pd.concat([df, pd.DataFrame(self.rows_to_add)], ignore_index=True)
            logging.info("Añadidas %s filas.", len(self.rows_to_add))
        return df
