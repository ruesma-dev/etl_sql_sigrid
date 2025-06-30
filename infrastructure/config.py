# infrastructure/table_config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:  # pylint: disable=too-few-public-methods
    # --- SQL Server ---
    SQL_SERVER      = os.getenv("SQL_SERVER", r"localhost\MSSQLSERVER01")
    SQL_DATABASE    = os.getenv("SQL_DATABASE", "TemporaryDB")
    SQL_DRIVER      = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
    INTEGRATED_AUTH = True   # siempre auth integrada

    # --- PostgreSQL ---
    PG_SERVER   = os.getenv("PG_SERVER", "localhost")
    PG_PORT     = int(os.getenv("PG_PORT", "5432"))
    PG_USER     = os.getenv("PG_USER", "postgres")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "admin")
    PG_DATABASE = os.getenv("PG_DATABASE", "clone_sigrid")
