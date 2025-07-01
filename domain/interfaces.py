# domain/interfaces.py
"""
Interfaces (contratos) usados por transformaciones específicas.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List
import pandas as pd


class SpecificTableTransformations(ABC):
    """
    Cada tabla que necesite lógica ETL muy particular puede definir
    una subclase de esta interfaz en:
        application/transformations/specific/<tabla>_transformations.py
    """

    @abstractmethod
    def get_table_transformations(self) -> List["BaseTransformation"]:
        """
        Devuelve una lista de instancias de BaseTransformation
        que se inyectarán al final de la cadena generada por
        TransformationFactory.
        """
        ...

# Nota:
# `BaseTransformation` se importa de manera perezosa en cada módulo que lo necesite
