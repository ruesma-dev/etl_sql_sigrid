# application/transformations/clean_null_chars_transformation.py
import pandas as pd
import logging
from application.transformations.base_transformation import BaseTransformation

class CleanNullCharsTransformation(BaseTransformation):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.replace("\x00", "", regex=False)
        logging.info("Eliminados caracteres NUL.")
        return df
