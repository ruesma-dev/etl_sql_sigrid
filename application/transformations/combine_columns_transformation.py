# application/transformations/combine_columns_transformation.py
import pandas as pd
import logging
from application.transformations.base_transformation import BaseTransformation

class CombineColumnsTransformation(BaseTransformation):
    def __init__(self, new_col: str, cols: list, sep: str = "_"):
        self.new_col, self.cols, self.sep = new_col, cols, sep

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if all(c in df.columns for c in self.cols):
            df[self.new_col] = df[self.cols].astype(str).agg(self.sep.join, axis=1)
            logging.info("Creada columna combinada %s.", self.new_col)
        return df
