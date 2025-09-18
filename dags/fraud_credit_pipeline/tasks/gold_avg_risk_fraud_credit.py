import duckdb
import logging

class GoldTaskAvgRisk:

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
        CREATE OR REPLACE TABLE gold_avg_risk_by_region AS

        SELECT
            CASE WHEN
                location_region = '' THEN 'UNKNOWN'
                ELSE location_region
            END AS location_region,
            ROUND(AVG(risk_score), 2) as avg_risk_score,
            COUNT(*) as total_transactions
        FROM
            silver_transactions
        WHERE
            location_region IS NOT NULL
        GROUP BY
            location_region
        ORDER BY
            avg_risk_score DESC
        """
        
        
        
        with duckdb.connect(database=self.db_path, read_only=False) as con:
            self.log.info("GOLD: Criando a tabela 'gold_avg_risk_by_region'...")
            con.sql(query)
            con.sql(f"""COPY (SELECT * FROM gold_avg_risk_by_region) TO '{self.output_parquet_path}' (FORMAT PARQUET);""")
            
            self.log.info("GOLD: Camada Gold criada com sucesso.")