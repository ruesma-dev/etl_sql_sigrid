# application/transformations/join_with_con_transformation.py
import pandas as pd
import logging
from application.transformations.base_transformation import BaseTransformation

class JoinWithConTransformation(BaseTransformation):
    def __init__(self, con_df: pd.DataFrame, join_col: str):
        self.con_df = con_df.rename(
            columns=lambda c: "con_ide" if c == "ide" else f"con_{c}"
        )
        self.join_col = join_col

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.merge(self.con_df, how="left", left_on=self.join_col, right_on="con_ide")
        logging.info("Join con 'con' completado.")
        return df
