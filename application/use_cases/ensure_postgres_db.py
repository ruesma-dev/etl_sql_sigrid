# application/use_cases/ensure_postgres_db.py
import logging

class EnsurePostgresDatabaseUseCase:
    def __init__(self, pg_gateway) -> None:
        self.pg = pg_gateway
        self.log = logging.getLogger(__name__)

    def execute(self) -> None:
        self.log.info("🛠️ Verificando BD PostgreSQL…")
        if not self.pg.database_exists():
            self.pg.create_database()
        self.pg.test_connection()
        self.log.info("✅ PostgreSQL OK.")
