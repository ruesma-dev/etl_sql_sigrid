# application/use_cases/transform_use_case.py
import pandas as pd
from application.transformations.transformation_factory import TransformationFactory

class TransformUseCase:
    """
    Aplica la cadena de transformaciones definida en TABLE_CONFIG.
    """
    def __init__(self, extract_uc):
        self.extract_uc = extract_uc     # para joins con 'con', etc.

    def __call__(self, table_key: str, df: pd.DataFrame) -> pd.DataFrame:
        trans = TransformationFactory.get_transformations(table_key, self.extract_uc)
        for t in trans:
            df = t.transform(df)
        return df
