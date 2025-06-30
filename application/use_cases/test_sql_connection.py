# application/use_cases/test_sql_connection.py
import logging

class TestSQLConnectionUseCase:
    def __init__(self, sql_gateway) -> None:
        self.sql = sql_gateway
        self.log = logging.getLogger(__name__)

    def execute(self) -> None:
        self.log.info("🛠️ Test conexión SQL Server…")
        self.sql.test_connection()
        self.log.info("✅ SQL Server OK.")
