# main.py
"""
Micro-servicio 2 – ETL Transform
1) Verifica conexión a SQL Server (auth integrada).
2) Comprueba si la BD destino existe en PostgreSQL; si no, la crea.
⚠️  Aún no ejecuta transformaciones – solo “smoke-checks”.
"""

from __future__ import annotations
import logging, sys

from infrastructure.config import Config
from infrastructure.sql_gateway import SQLServerGateway
from infrastructure.pg_gateway import PostgresAdminGateway
from application.use_cases.test_sql_connection import TestSQLConnectionUseCase
from application.use_cases.ensure_postgres_db import EnsurePostgresDatabaseUseCase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
)
logger = logging.getLogger("main")

def main() -> None:
    try:
        # 1️⃣ SQL Server
        sql_gw = SQLServerGateway(config=Config)
        TestSQLConnectionUseCase(sql_gw).execute()

        # 2️⃣ PostgreSQL
        pg_admin = PostgresAdminGateway(config=Config)
        EnsurePostgresDatabaseUseCase(pg_admin).execute()

        logger.info("🏁 Checks iniciales completados con éxito.")
    except Exception as exc:               # pylint: disable=broad-except
        logger.error("🔥 Fallo en checks iniciales: %s", exc, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
