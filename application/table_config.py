# application/table_config.py
"""
Mapa con las reglas ETL por tabla:

- source_table: nombre en SQL Server
- target_table: nombre deseado en PostgreSQL
- primary_key:  clave primaria en destino
- rename_columns:  {src: dst}
- date_columns:  lista de columnas a convertir a date/datetime
- foreign_keys:   para validaciones o FK en Postgres
- join_with_con:  info para joins con la tabla `con`
- data_cleaning:  directivas de limpieza
- combine_columns: creación de columnas nuevas combinando otras
"""

TABLE_CONFIG = {
    'obr': {
        'source_table': 'obr',
        'target_table': 'FactObra',
        'primary_key': 'ide',
        'rename_columns': {
            'res': 'nombre_obra'
        },
        'date_columns': [
            'fecinipre', 'fecfinpre', 'fecinirea', 'fecfinrea',
            'feccieest', 'fecadj', 'fecfincie', 'fecciepre',
            'fecapelic', 'feclic', 'fecofe'
        ],
        'foreign_keys': [
            {
                'column': 'cenide',
                'ref_table': 'DimCentroCoste',
                'ref_column': 'ide'
            }
        ],
        'data_cleaning': {
            'handle_invalid_foreign_keys': 'add_placeholder'
        },
        # Hacemos join con 'con' usando cenide = con.ide (según lo solicitado)
        'join_with_con': {
            'join_column': 'cenide'
        }
    },
    'cen': {
        'source_table': 'cen',
        'target_table': 'DimCentroCoste',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fecfinpre', 'fecfinrea', 'fecbloqueo', 'fecfincie'],
        'foreign_keys': [],
        'data_cleaning': {
            'add_placeholder_row': True
        }
    },
    'con': {
        'source_table': 'con',
        'target_table': 'DimConceptosETC',
        'primary_key': 'ide',
        'rename_columns': {
            'fec': 'fecha_alta',
            'fecbaj': 'fecha_baja',
        },
        'date_columns': ['fec','fecbaj'],
        'foreign_keys': [],
        'data_cleaning': {},
        'combine_columns': [
            {
                'new_column_name': 'primary_ide',
                'columns_to_combine': ['tip', 'est'],
                'separator': '_'
            }
        ]
    },
    'auxobrtip': {
        'source_table': 'auxobrtip',
        'target_table': 'DimTipoObra',
        'primary_key': 'ide',
        'rename_columns': {
            'fecbaj': 'fecha_baja'
        },
        'date_columns': ['fecbaj'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'auxobrcla': {
        'source_table': 'auxobrcla',
        'target_table': 'DimSubtipoObra',
        'primary_key': 'ide',
        'rename_columns': {
            'fecbaj': 'fecha_baja'
        },
        'date_columns': ['fecbaj'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'conest': {
        'source_table': 'conest',
        'target_table': 'DimEstadoConcepto',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
        'data_cleaning': {},
        'combine_columns': [
            {
                'new_column_name': 'primary_ide',
                'columns_to_combine': ['tip', 'est'],
                'separator': '_'
            }
        ]
    },
    'dca': {
        'source_table': 'dca',
        'target_table': 'DimAlbaranCompra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'fecpag', 'fecent', 'feclim', 'fecrec',
            'fecfac', 'alqfec', 'divcamfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {},
        'join_with_con': {
            'join_column': 'ide'
        }
    },
    'ctr': {
        'source_table': 'ctr',
        'target_table': 'DimContratoCompra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'fecpag', 'fecent', 'feclim', 'fecfac',
            'divcamfec', 'fecvig1', 'fecvig2', 'fecrevpre'
        ],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de ide
        'join_with_con': {
            'join_column': 'ide'
        }
    },
    'dcapro': {
        'source_table': 'dcapro',
        'target_table': 'DimAlbaranCompraProductos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec','garfec','fecimp'],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de docide
        'join_with_con': {
            'join_column': 'docide'
        }
    },
    'dcaproana': {
        'source_table': 'dcaproana',
        'target_table': 'DimAlbaranCompraAnalitica',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcaprodes': {
        'source_table': 'dcaprodes',
        'target_table': 'DimAlbaranCompraDestinos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcapropar': {
        'source_table': 'dcapropar',
        'target_table': 'DimAlbaranCompraPartidas',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcaproser': {
        'source_table': 'dcaproser',
        'target_table': 'DimAlbaranCompraProductosSeries',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dcarec': {
        'source_table': 'dcarec',
        'target_table': 'DimAlbaranCompraRecargos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'obrcer': {
        'source_table': 'obrcer',
        'target_table': 'DimCertificacionObra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'obrcos': {
        'source_table': 'obrcos',
        'target_table': 'DimCentroCosteObra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['orifec'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'obrctr': {
        'source_table': 'obrctr',
        'target_table': 'DimContratoObra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecprelic', 'fecrealic', 'fecpreadj', 'fecreaadj',
            'fecprefir', 'fecreafir', 'fecprefct', 'fecreafct',
            'fecpreact', 'fecreaact', 'fecpreini', 'fecreaini',
            'fecprefin', 'fecreafin', 'fecprosol', 'fecprorec',
            'fecproliq', 'fecproapr', 'fecdefsol', 'fecdefrec',
            'fecdefliq', 'fecdefapr', 'fecinipla', 'fecinigar',
            'fecfingar', 'fecdevret', 'fecaprtec', 'fecapreco',
            'fecultsit', 'fecrevpre'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'obrparpar': {
        'source_table': 'obrparpar',
        'target_table': 'DimPartidasObra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fecini','fecfin'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'cer': {
        'source_table': 'cer',
        'target_table': 'DimCertificacion',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'feccob', 'fecent', 'feclim', 'fecfac',
            'divcamfec', 'alqfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'cerpro': {
        'source_table': 'cerpro',
        'target_table': 'DimCertificacionProductos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec', 'garfec', 'fecimp'],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de docide
        'join_with_con': {
            'join_column': 'docide'
        }
    },
    'cob': {
        'source_table': 'cob',
        'target_table': 'DimCobro',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fecven', 'fecrea', 'fecreaemi', 'divcamfec'],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dvf': {
        'source_table': 'dvf',
        'target_table': 'DimFacturaVenta',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'feccob', 'fecent', 'feclim', 'fecfac',
            'divcamfec', 'alqfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'dvfpro': {
        'source_table': 'dvfpro',
        'target_table': 'DimFacturaVentaProductos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fec', 'garfec', 'fecimp', 'fec1', 'fec2',
            'perfini', 'perffin', 'alqfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de docide
        'join_with_con': {
            'join_column': 'docide'
        }
    },
    'pro': {
        'source_table': 'pro',
        'target_table': 'DimProducto',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fectipdes', 'fecult', 'fecser', 'fecrec', 'fecultact',
            'esigpromf1','esigpromf2','esigpliqf1','esigpliqf2',
            'esigpnovf1','esigpnovf2'
        ],
        'foreign_keys': [],
        'data_cleaning': {}
    },
    'cli': {
        'source_table': 'cli',
        'target_table': 'DimCliente',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'creevafec','evafec','fecnac','dopfec1','dopfec2','abofec1','abofec2'
        ],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de ide
        'join_with_con': {
            'join_column': 'ide'
        }
    },
    'prv': {
        'source_table': 'prv',
        'target_table': 'DimProveedor',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fecnac','cerfeccad','dopfec1','dopfec2'],
        'foreign_keys': [],
        'data_cleaning': {},
        # Join con con a través de ide
        'join_with_con': {
            'join_column': 'ide'
        }
    },
    'dcf': {
        'source_table': 'dcf',
        'target_table': 'DimFacturaCompra',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': [
            'fecdoc', 'fecpag', 'fecent', 'feclim', 'fecrec',
            'fecfac', 'alqfec', 'divcamfec'
        ],
        'foreign_keys': [],
        'data_cleaning': {},
        'join_with_con': {
            'join_column': 'ide'
        }
    },
    'dcfpro': {
        'source_table': 'dcfpro',
        'target_table': 'DimFacturaCompraProductos',
        'primary_key': 'ide',
        'rename_columns': {},
        'date_columns': ['fec','garfec','fecimp'],
        'foreign_keys': [],
        'data_cleaning': {},
        # join con con a traves de docide
        'join_with_con': {
            'join_column': 'docide'
        }
    },
    'tar': {
        'source_table': 'tar',  # Nombre de la tabla en la base de datos de origen
        'target_table': 'DimTarea',  # Ajusta el nombre según tu convención
        'primary_key': 'ide',
        'rename_columns': {
            # Si deseas renombrar columnas, agrégalos aquí
            # 'numord': 'numero_orden'
        },
        'date_columns': [
            'fecestini',   # Fecha inicio estimada
            'fecestfin',   # Fecha final estimada
            'feclim',      # Fecha límite
            'fecreaini',   # Fecha inicio real
            'fecreafin',   # Fecha final real
        ],
        'foreign_keys': [
        ],
        'data_cleaning': {},
        'join_with_con': {
            'join_column': 'obride'  # La columna en tar que se usará para machar con con.ide
        }
    },
    'obrparpre': {
        'source_table': 'obrparpre',  # Nombre de la tabla en la base de datos de origen
        'target_table': 'DimPresupuestoMediciones',  # Ajusta el nombre según tu convención
        'primary_key': 'ide',
        'rename_columns': {
            # Si deseas renombrar columnas, agrégalas aquí
            # Ejemplo: 'tex': 'descripcion_larga'
        },
        'date_columns': [
            # No hay columnas de tipo fecha en esta tabla
        ],
        'foreign_keys': [
        ],
        'data_cleaning': {},
        'join_with_con': {
        }
    },
    'auxobramb': {
        'source_table': 'auxobramb',            # Nombre de la tabla en la BBDD origen
        'target_table': 'DimAmbitoObra',        # Cambia si prefieres otro nombre de tabla destino
        'primary_key': 'ide',
        'rename_columns': {
            'fecbaj': 'fecha_baja',
            'res': 'resumen_ambito'
        },
        'date_columns': [
            'fecbaj'
        ],
        'foreign_keys': [
        ],
        'data_cleaning': {
            # Opciones de limpieza específicas si fuera necesario
        }
    },
    'obrfas': {
        'source_table': 'obrfas',  # Nombre de la tabla en la BBDD origen
        'target_table': 'DimObraFases',  # Cambia si prefieres otro nombre de tabla destino
        'primary_key': 'ide',
        'rename_columns': {
            'fecini': 'fecha_inicio',
            'fecfin': 'fecha_fin',
            'res': 'nombre_fase'
        },
        'date_columns': [
            'fecini', 'fecfin'
        ],
        'foreign_keys': [
        ],
        'data_cleaning': {
            # Opciones de limpieza específicas si fuera necesario
        }
    },
    'hmores': {
        'source_table': 'hmores',
        'target_table': 'DimPartesTrabajoDetalle',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {
            # Añade renombrados de columnas si lo necesitas, ejemplo:
            # 'tex': 'descripcion_detalle'
        },
        'date_columns': [
            'fec',     # Fecha principal
            'fec1',    # Fecha1 alquiler
            'fec2'     # Fecha2 alquiler
        ],
        'foreign_keys': [
            # Añade claves foráneas si procede, por ejemplo:
            # {
            #     'column': 'obride',
            #     'ref_table': 'FactObra',
            #     'ref_column': 'ide'
            # }
        ],
        'data_cleaning': {
            # Ajusta opciones de limpieza si es necesario
        }
    },
    'hmo': {
        'source_table': 'hmo',
        'target_table': 'DimPartesTrabajo',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {
            # Añade renombrados de columnas si lo necesitas, ejemplo:
            # 'reside': 'recurso_asociado'
        },
        'date_columns': [
            'feccie'   # Fecha de cierre diario
        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'cve': {
        'source_table': 'hmo',
        'target_table': 'DimContratoVenta',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [
        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'obrfasamb': {
        'source_table': 'obrfasamb',
        'target_table': 'DimFasesAmbito',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [
            'feccie', 'fec', 'plafec', 'ofefecpre'
        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'obrlba': {
        'source_table': 'obrlba',
        'target_table': 'DimLineasPlanifacion',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [
            'fec'
        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'obrlbatar': {
        'source_table': 'obrlbatar',
        'target_table': 'DimObraTareas',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [
            'fecini', 'fecfin'
        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'obrpas': {
        'source_table': 'obrpas',
        'target_table': 'DimPasos',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [
            'fecini', 'fecfin'
        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'obrper': {
        'source_table': 'obrper',
        'target_table': 'DimPeriodos',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [
            'fecini', 'fecfin'
        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'rob': {
        'source_table': 'rob',
        'target_table': 'DimMensual',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [

        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'conext': {
        'source_table': 'conext',
        'target_table': 'DimCamposExtValores',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [

        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'defext': {
        'source_table': 'defext',
        'target_table': 'DimCamposExtDefinicion',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [

        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'emp': {
        'source_table': 'emp',
        'target_table': 'DimEmpleados',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [

        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'age': {
        'source_table': 'age',
        'target_table': 'DimAgentes',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [

        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'res': {
        'source_table': 'res',
        'target_table': 'DimRecursos',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [

        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
    'auxhor': {
        'source_table': 'auxhor',
        'target_table': 'DimTipoHora',  # Ajusta el nombre a tu convención
        'primary_key': 'ide',
        'rename_columns': {

        },
        'date_columns': [

        ],
        'foreign_keys': [
            # Añade claves foráneas si procede
        ],
        'data_cleaning': {
            # Opciones de limpieza, si las requieres
        }
    },
}
