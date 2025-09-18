import duckdb
import logging

class SilverTask:

    def __init__(self, db_path: str, output_parquet_path: str):
        self.db_path = db_path
        self.output_parquet_path = output_parquet_path
        self.log = logging.getLogger(__name__)

    def execute(self) -> None:
        """
        Lê da tabela Bronze, aplica limpezas e transformações.
        """
        self.log.info(f"SILVER: Iniciando transformações no banco de dados '{self.db_path}'")
        
        query = """
        CREATE OR REPLACE TABLE silver_transactions AS
        
        WITH raw AS (
            SELECT 
                * FROM bronze_transactions
        )
        SELECT
            TO_TIMESTAMP("timestamp") AS timestamp,
            TRIM(sending_address) AS sending_address,
            TRIM(receiving_address) AS receiving_address,
            COALESCE(ROUND(TRY_CAST(amount AS DOUBLE), 2), 0) AS amount,
            TRIM(UPPER(transaction_type)) AS transaction_type,
            REGEXP_REPLACE(TRIM(UPPER(location_region)), '\d', '', 'g') AS location_region,
            TRIM(ip_prefix) AS ip_prefix,
            login_frequency,
            session_duration,
            TRIM(UPPER(purchase_pattern)) AS purchase_pattern,
            TRIM(UPPER(age_group)) AS age_group,
            ROUND(TRY_CAST(risk_score AS DOUBLE), 2) AS risk_score,
            TRIM(UPPER(anomaly)) AS anomaly
        FROM
            raw
        """
        
        with duckdb.connect(database=self.db_path, read_only=False) as con:
            con.sql(query)
            con.sql(f"""COPY (SELECT * FROM silver_transactions) TO '{self.output_parquet_path}' (FORMAT PARQUET);""")
            
        self.log.info("SILVER: Tabela 'silver_transactions' criada com sucesso.")