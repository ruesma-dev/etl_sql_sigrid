# application/transformations/transformation_factory.py
from __future__ import annotations
import logging, importlib, importlib.util
from typing import List

from application.table_config import TABLE_CONFIG

# Transformaciones genéricas
from application.transformations.add_rows_transformation import AddRowsTransformation
from application.transformations.delete_rows_transformation import DeleteRowsTransformation
from application.transformations.date_transformation import DateTransformation
from application.transformations.drop_null_or_zero_columns_transformation import (
    DropNullOrZeroColumnsTransformation,
)
from application.transformations.rename_columns_transformation import RenameColumnsTransformation
from application.transformations.sort_columns_transformation import SortColumnsTransformation
from application.transformations.combine_columns_transformation import CombineColumnsTransformation
from application.transformations.join_with_con_transformation import JoinWithConTransformation
from application.transformations.clean_null_chars_transformation import CleanNullCharsTransformation

from application.transformations.row_modifications_mapping import (
    ROW_INSERTION_MAPPING,
    ROW_DELETION_MAPPING,
)

# Interfaz para transformaciones específicas
from domain.interfaces import SpecificTableTransformations

logger = logging.getLogger(__name__)


class TransformationFactory:
    """
    Devuelve la lista de transformaciones a aplicar a cada tabla.
    Gestiona caches de DataFrames auxiliares (p.e. 'con', 'dcfpro').
    """

    # caches estáticos
    con_df = None
    dcfpro_df = None

    # guardamos extract_use_case para cargar datasets auxiliares
    extract_use_case = None

    # --------------------------------------------------------------------- #
    @staticmethod
    def load_con_df_if_needed():
        if TransformationFactory.con_df is None:
            if TransformationFactory.extract_use_case is None:
                raise ValueError("extract_use_case no configurado (para 'con').")
            logger.info("Cargando tabla 'con' en memoria …")
            df_map = TransformationFactory.extract_use_case(["con"])
            TransformationFactory.con_df = df_map.get("con")

    # --------------------------------------------------------------------- #
    @staticmethod
    def load_dcfpro_df_if_needed():
        if TransformationFactory.dcfpro_df is None:
            if TransformationFactory.extract_use_case is None:
                raise ValueError("extract_use_case no configurado (para 'dcfpro').")
            logger.info("Cargando tabla 'dcfpro' en memoria …")
            df_map = TransformationFactory.extract_use_case(["dcfpro"])
            TransformationFactory.dcfpro_df = df_map.get("dcfpro")

    # --------------------------------------------------------------------- #
    @staticmethod
    def get_transformations(table_key: str, extract_uc=None) -> List:
        """
        Devuelve lista de instancias de transformaciones
        para la clave `table_key`.
        """
        if extract_uc and TransformationFactory.extract_use_case is None:
            TransformationFactory.extract_use_case = extract_uc

        if table_key not in TABLE_CONFIG:
            logger.warning("Sin configuración para '%s'.", table_key)
            return []

        cfg = TABLE_CONFIG[table_key]
        transformations: List = []

        # 1) join con 'con' (si procede)
        join_cfg = cfg.get("join_with_con")
        if join_cfg:
            if TransformationFactory.con_df is None:
                TransformationFactory.load_con_df_if_needed()
            if TransformationFactory.con_df is not None:
                transformations.append(JoinWithConTransformation(
                    TransformationFactory.con_df,
                    join_cfg["join_column"],
                ))

        # 2) filas a insertar / eliminar
        target = cfg["target_table"]
        if ROW_INSERTION_MAPPING.get(target):
            transformations.append(AddRowsTransformation(
                ROW_INSERTION_MAPPING[target]
            ))
        if ROW_DELETION_MAPPING.get(target):
            transformations.append(DeleteRowsTransformation(
                ROW_DELETION_MAPPING[target]
            ))

        # 3) fechas
        if cfg.get("date_columns"):
            col_map = {c: c for c in cfg["date_columns"]}
            transformations.append(DateTransformation(col_map))

        # 4) combinar columnas
        for cmb in cfg.get("combine_columns", []):
            transformations.append(CombineColumnsTransformation(
                cmb["new_column_name"],
                cmb["columns_to_combine"],
                cmb.get("separator", "_"),
            ))

        # 5) limpieza columnas vacías / nulas
        transformations.append(DropNullOrZeroColumnsTransformation())

        # 6) rename
        if cfg.get("rename_columns"):
            transformations.append(RenameColumnsTransformation(
                cfg["rename_columns"]
            ))

        # 7) ordenar columnas
        transformations.append(SortColumnsTransformation())

        # 8) transformaciones específicas (opcionales)
        mod_name = f"application.transformations.specific.{table_key}_transformations"
        cls_name = f"{table_key.capitalize()}Transformations"
        spec = importlib.util.find_spec(mod_name)

        if spec is not None:
            mod = importlib.import_module(mod_name)
            TransformCls = getattr(mod, cls_name, None)
            if TransformCls and issubclass(TransformCls, SpecificTableTransformations):
                # caso especial dcf / dcfpro
                if table_key == "dcf":
                    TransformationFactory.load_dcfpro_df_if_needed()
                    if TransformationFactory.dcfpro_df is not None:
                        instance = TransformCls(TransformationFactory.dcfpro_df)
                    else:
                        instance = None
                # otros casos
                else:
                    instance = TransformCls()
                if instance:
                    transformations.extend(instance.get_table_transformations())

        # 9) limpieza de caracteres NUL
        transformations.append(CleanNullCharsTransformation())

        return transformations
