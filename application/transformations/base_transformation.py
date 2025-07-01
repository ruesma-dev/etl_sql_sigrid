# application/transformations/base_transformation.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformation(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        ...
