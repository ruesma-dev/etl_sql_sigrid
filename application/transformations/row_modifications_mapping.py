# application/transformations/row_modifications_mapping.py
"""
Mapas de inserción / eliminación de filas que se aplican dentro de
TransformationFactory.

• ROW_INSERTION_MAPPING →
       {<tabla_destino>: [ {col: val, …}, … ] }

• ROW_DELETION_MAPPING →
       {<tabla_destino>: [ {col: val, …}, … ] }
"""

ROW_INSERTION_MAPPING = {
    # Ejemplo: insertar fila “centro coste 0”
    "DimCentroCoste": [
        {"ide": 0, "cenide": 0, "des": "Centro coste genérico"},
    ],
    # añade más tablas según sea necesario …
}

ROW_DELETION_MAPPING = {
    # Ejemplo: eliminar fila con ide=496414
    "DimCentroCoste": [
        {"ide": 496414},
    ],
    # añade más tablas según sea necesario …
}
