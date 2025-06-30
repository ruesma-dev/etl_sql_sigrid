# infrastructure/pg_gateway.py
from __future__ import annotations
import logging, psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from infrastructure.config import Config

logger = logging.getLogger(__name__)

class PostgresAdminGateway:
    """
    Permite comprobar y crear la BD destino.
    """
    def __init__(self, *, config: type[Config]) -> None:
        self.host, self.port = config.PG_SERVER, config.PG_PORT
        self.user, self.password = config.PG_USER, config.PG_PASSWORD
        self.dbname = config.PG_DATABASE

    # --------------------------------------------------
    def _pg_conn(self, db: str):
        return psycopg2.connect(
            host=self.host, port=self.port,
            user=self.user, password=self.password,
            dbname=db,
        )

    # --------------------------------------------------
    def database_exists(self) -> bool:
        with self._pg_conn("postgres") as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.dbname,),
            )
            exists = cur.fetchone() is not None
        logger.info(
            "La base '%s' %s en PostgreSQL.",
            self.dbname,
            "existe" if exists else "no existe",
        )
        return exists

    # --------------------------------------------------
    def create_database(self) -> None:
        """
        Crea la base de datos si no existe (requiere autocommit).
        """
        logger.info("Creando base '%s'…", self.dbname)

        conn = psycopg2.connect(
            host=self.host, port=self.port,
            user=self.user, password=self.password,
            dbname="postgres",
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # ← clave
        cur = conn.cursor()
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.dbname)))
        cur.close()
        conn.close()

        logger.info("Base '%s' creada.", self.dbname)

    # --------------------------------------------------
    def test_connection(self) -> None:
        """
        Conecta a la base destino y corre SELECT 1.
        """
        logger.info("Conectando a PostgreSQL db=%s…", self.dbname)

        with self._pg_conn(self.dbname) as conn:
            cur = conn.cursor()          # ← 1. crea el cursor
            cur.execute("SELECT 1")      # ← 2. ejecuta
            row = cur.fetchone()         # ← 3. fetch
            cur.close()

        if row is None or row[0] != 1:
            raise RuntimeError("SELECT 1 en PostgreSQL devolvió valor inesperado.")
        logger.info("Conexión a PostgreSQL OK.")
