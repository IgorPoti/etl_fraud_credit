import duckdb
import logging

class BronzeTask:

    def __init__(self, db_path: str, csv_path: str, output_parquet_path: str):
        self.db_path = db_path
        self.csv_path = csv_path
        self.output_parquet_path = output_parquet_path
        self.log = logging.getLogger(__name__)

    def execute(self):
        """
        Ponto de entrada para a execução da tarefa no Airflow.
        Lê o arquivo CSV bruto e o carrega em uma tabela no DuckDB.
        """
        self.log.info(f"BRONZE: Iniciando ingestão do CSV '{self.csv_path}' para o banco '{self.db_path}'")
        
        query = f"""
        CREATE OR REPLACE TABLE bronze_transactions AS
        SELECT 
            timestamp,
            sending_address,
            receiving_address,
            amount,
            transaction_type,
            location_region,
            TRY_CAST(ip_prefix AS VARCHAR) AS ip_prefix,
            login_frequency,
            session_duration,
            purchase_pattern,
            age_group,
            risk_score,
            anomaly
        FROM read_csv_auto('{self.csv_path}');
        """
        
        with duckdb.connect(database=self.db_path, read_only=False) as con:
            con.sql(query)
            con.sql(f"""COPY (SELECT * FROM read_csv_auto('{self.csv_path}', header=true))
        TO '{self.output_parquet_path}'
        (FORMAT PARQUET);""")
        
        self.log.info("BRONZE: Tabela 'bronze_transactions' criada com sucesso.")