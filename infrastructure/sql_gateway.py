# infrastructure/sql_gateway.py
from __future__ import annotations
import logging, pyodbc
from infrastructure.config import Config

logger = logging.getLogger(__name__)

class SQLServerGateway:
    """
    Pequeño gateway para validar driver y credenciales
    mediante `SELECT 1` a master.
    Solo soporta autenticación integrada.
    """
    def __init__(self, *, config: type[Config]) -> None:
        self.server   = config.SQL_SERVER
        self.driver   = config.SQL_DRIVER
        self.database = config.SQL_DATABASE

    # --------------------------------------------------
    def test_connection(self) -> None:
        if self.driver not in pyodbc.drivers():
            raise RuntimeError(f"ODBC driver '{self.driver}' no instalado.")

        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            "DATABASE=master;"
            "Trusted_Connection=yes;"
        )

        logger.info("Conectando a SQL Server %s / base %s…",
                    self.server, self.database)

        with pyodbc.connect(conn_str, timeout=5) as cn:
            cur = cn.cursor()
            cur.execute("SELECT 1")
            if cur.fetchone()[0] != 1:
                raise RuntimeError("SELECT 1 devolvió valor inesperado.")
        logger.info("Conexión a SQL Server OK.")



