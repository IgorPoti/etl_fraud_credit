import logging
import duckdb
from airflow.exceptions import AirflowException

class DataQualityTask:
    """
    Executa testes de qualidade de dados de forma customizada
    """
    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        self.log = logging.getLogger(__name__)
        self.checks = [
            {
                'name': 'A tabela não deve estar vazia',
                'query': f"SELECT COUNT(*) FROM {self.table_name}",
                'validation': lambda result: result > 0,
                'error_message': f"A tabela '{self.table_name}' está vazia."
            },
            {
                'name': "A coluna 'timestamp' não pode ter nulos",
                'query': f"SELECT COUNT(*) FROM {self.table_name} WHERE timestamp IS NULL",
                'validation': lambda result: result == 0,
                'error_message': f"Encontrados valores nulos na coluna 'timestamp'."
            },
            {
                'name': "O percentual de 'amount' nulo deve ser < 1%",
                'query': f"SELECT CAST(SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) FROM {self.table_name}",
                'validation': lambda result: (result * 100) < 1.0,
                'error_message': f"O percentual de conformidade para 'amount' nulo falhou. (Percentual de erro encontrado: {{result:.2%}})"
            },
            {
                'name': "A coluna 'transaction_type' deve ter apenas valores válidos",
                'query': f"SELECT COUNT(*) FROM {self.table_name} WHERE transaction_type NOT IN ('PURCHASE', 'SALE', 'TRANSFER', 'PHISHING', 'SCAM')",
                'validation': lambda result: result == 0,
                'error_message': f"Encontrados valores inválidos na coluna 'transaction_type'."
            },
            {
                'name': "A coluna 'amount' não pode ter valores negativos",
                'query': f"SELECT COUNT(*) FROM {self.table_name} WHERE amount < 0",
                'validation': lambda result: result == 0,
                'error_message': f"Encontrados valores negativos na coluna 'amount'."
            },
            {
                'name': "A coluna 'risk_score' deve estar entre 0 e 100",
                'query': f"SELECT COUNT(*) FROM {self.table_name} WHERE risk_score < 0 OR risk_score > 100",
                'validation': lambda result: result == 0,
                'error_message': f"Encontrados valores de 'risk_score' fora do intervalo [0, 100]."
            },
            {
                'name': "A média do 'risk_score' deve estar em um intervalo esperado",
                'query': f"SELECT ROUND(AVG(risk_score), 2) FROM {self.table_name}",
                'validation': lambda avg_score: 30 <= avg_score <= 60,
                'error_message': f"A média do 'risk_score' está fora do esperado [30, 60]. (Média encontrada: {{result:.2f}})"
            }
        ]

    def execute(self):
        self.log.info(f"DATA QUALITY: Iniciando validações para a tabela '{self.table_name}'...")
        failed_checks = []
        
        with duckdb.connect(database=self.db_path, read_only=True) as con:
            for check in self.checks:
                self.log.info(f"Executando verificação: {check['name']}")
                try:
                    result = con.sql(check['query']).fetchone()[0]
                    result = result if result is not None else 0
                    
                    if not check['validation'](result):
                        error_message = check['error_message'].format(result=result)
                        error = f"Falha na verificação '{check['name']}': {error_message}"
                        self.log.error(error)
                        failed_checks.append(error)
                    else:
                        self.log.info(f"Verificação '{check['name']}' passou. (Resultado: {result})")
                except Exception as e:
                    error = f"Falha na EXECUÇÃO da verificação '{check['name']}': {e}"
                    self.log.error(error)
                    failed_checks.append(error)

        if failed_checks:
            full_error_message = "\n".join(failed_checks)
            raise AirflowException(f"Uma ou mais verificações de qualidade de dados falharam:\n{full_error_message}")
        
        self.log.info(f"Todas as verificações de qualidade de dados para a tabela '{self.table_name}' passaram com sucesso!")