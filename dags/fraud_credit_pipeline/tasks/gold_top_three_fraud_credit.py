import duckdb
import logging

class GoldTaskTopThreeLatestSales:

    def __init__(self, db_path: str, output_parquet_path: str):
        self.db_path = db_path
        self.output_parquet_path = output_parquet_path
        self.log = logging.getLogger(__name__)

    def execute(self):
        """
        Cria tabelas agregadas para consumo final.
        """
        self.log.info(f"GOLD: Iniciando agregações no banco de dados '{self.db_path}'")

        query =  """
        CREATE OR REPLACE TABLE gold_top_3_latest_sales AS

        WITH raw AS (
            SELECT
                receiving_address,
                amount,
                timestamp,
                ROW_NUMBER() OVER(PARTITION BY receiving_address ORDER BY timestamp DESC) as sale_rank
            FROM
                silver_transactions
            WHERE
                transaction_type = 'SALE'
        )
        SELECT
            receiving_address,
            amount,
            timestamp
        FROM
            raw
        WHERE
            sale_rank = 1
        ORDER BY
            amount DESC
        LIMIT 3
        """
        
        
        
        with duckdb.connect(database=self.db_path, read_only=False) as con:
            self.log.info("GOLD: Criando a tabela 'gold_top_3_latest_sales'...")
            con.sql(query)
            con.sql(f"""COPY (SELECT * FROM gold_top_3_latest_sales) TO '{self.output_parquet_path}' (FORMAT PARQUET);""")

            self.log.info("GOLD: Camada Gold TOP 3 LATEST SALES criada com sucesso.")