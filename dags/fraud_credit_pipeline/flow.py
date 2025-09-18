from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from fraud_credit_pipeline.tasks.bronze_fraud_credit import BronzeTask
from fraud_credit_pipeline.tasks.silver_fraud_credit import SilverTask
from fraud_credit_pipeline.tasks.gold_avg_risk_fraud_credit import GoldTaskAvgRisk
from fraud_credit_pipeline.tasks.gold_top_three_fraud_credit import GoldTaskTopThreeLatestSales
from fraud_credit_pipeline.tasks.data_quality import DataQualityTask


DB_PATH = "/opt/airflow/storage/db/fraud_credit.db"
CSV_PATH = "/opt/airflow/storage/landing/df_fraud_credit.csv"
BRONZE_PARQUET_PATH = "/opt/airflow/storage/bronze/bronze_fraud_credit.parquet"
SILVER_PARQUET_PATH = "/opt/airflow/storage/silver/silver_fraud_credit.parquet"
GOLD_THREE_PARQUET_PATH = "/opt/airflow/storage/gold/gold_top_three_credit.parquet"
GOLD_AVG_RISK_PARQUET_PATH = "/opt/airflow/storage/gold/gold_avg_risk_fraud_credit.parquet"

SODA_CONNECTION_PATH = "/opt/airflow/dags/soda_configs/conexao.yml"
SODA_CHECKS_PATH = "/opt/airflow/dags/soda_configs/validacoes_silver.yml"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    dag_id='fraud_credit_pipeline',
    default_args=default_args,
    description='Pipeline de dados de fraude com DuckDB',
    schedule=None,
    catchup=False,
    tags=['duckdb', 'fraud'],
) as dag:

    start = EmptyOperator(task_id='start')

    bronze = PythonOperator(
        task_id='bronze',
        python_callable=BronzeTask(db_path=DB_PATH, csv_path=CSV_PATH, output_parquet_path=BRONZE_PARQUET_PATH).execute
    )

    silver = PythonOperator(
        task_id='silver',
        python_callable=SilverTask(db_path=DB_PATH, output_parquet_path=SILVER_PARQUET_PATH).execute
    )

    dq_silver = PythonOperator(
            task_id='dq_silver',
            python_callable=DataQualityTask(
            db_path=DB_PATH, 
            table_name='silver_transactions'
            ).execute
        )

    gold_avg_risk = PythonOperator(
        task_id='gold_avg_risk',
        python_callable=GoldTaskAvgRisk(db_path=DB_PATH, output_parquet_path=GOLD_AVG_RISK_PARQUET_PATH).execute
    )
    
    gold_top_three = PythonOperator(
        task_id='gold_top_three',
        python_callable=GoldTaskTopThreeLatestSales(db_path=DB_PATH, output_parquet_path=GOLD_THREE_PARQUET_PATH).execute
    )

    end = EmptyOperator(task_id='end')

    start >> bronze >> silver >> dq_silver >> [gold_avg_risk, gold_top_three] >> end
