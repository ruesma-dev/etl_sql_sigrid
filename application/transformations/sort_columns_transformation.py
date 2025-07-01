# application/transformations/sort_columns_transformation.py
import logging
from application.transformations.base_transformation import BaseTransformation

class SortColumnsTransformation(BaseTransformation):
    def __init__(self, ascending: bool = True):
        self.ascending = ascending

    def transform(self, df):
        sorted_cols = sorted(df.columns, reverse=not self.ascending)
        logging.info("Columnas ordenadas.")
        return df[sorted_cols]
