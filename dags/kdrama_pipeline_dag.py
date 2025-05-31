from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import os
from datetime import datetime

# Caminho para o diretório que contém src_project e data_project (se aplicável)
# ou diretamente o diretório onde os scripts serão executados.
# Com os volumes: ./src:/opt/airflow/src_project e ./data:/opt/airflow/data_project
# e os scripts em src_project/pipelines/

PROJECT_SRC_IN_CONTAINER = "/opt/airflow/src_project"
PYTHON_EXECUTABLE = "python"

# Scripts paths relative to PROJECT_SRC_IN_CONTAINER
BRONZE_SCRIPT_RELATIVE_PATH = "pipelines/bronze.py"
SILVER_SCRIPT_RELATIVE_PATH = "pipelines/silver.py"
GOLD_SCRIPT_RELATIVE_PATH = "pipelines/gold.py"

# Adiciona /opt/airflow/src_project ao PYTHONPATH para que common.api_client possa ser importado
# e define o diretório de trabalho como /opt/airflow para que os scripts
# que calculam PROJECT_ROOT como '../../' cheguem a /opt/airflow
# e possam então acessar /opt/airflow/data_project (se os scripts usarem essa lógica)
# Seus scripts já calculam o PROJECT_ROOT e depois data/pasta, então:
# PROJECT_ROOT nos scripts será /opt/airflow/src_project/.. -> /opt/airflow
# E o caminho para dados será /opt/airflow/data_project/bronze etc.

# Comando prefixo para garantir que o PYTHONPATH esteja configurado
# E que o script seja executado a partir da raiz do código fonte montado.
COMMAND_PREFIX = f"export PYTHONPATH=$PYTHONPATH:{PROJECT_SRC_IN_CONTAINER}; cd {PROJECT_SRC_IN_CONTAINER} && "
# Ou se os scripts já lidam com os caminhos de dados corretamente a partir de sua própria localização:
# COMMAND_PREFIX = f"export PYTHONPATH=$PYTHONPATH:{PROJECT_SRC_IN_CONTAINER}; "


default_args = {
    'owner': 'Sisnando Junior', # Alterado
    'email': 'nando.devs@gmail.com', # Alterado
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=1), # Reduzido
}

with DAG(
    dag_id="kdrama_pipeline_final_docker", # Novo ID para garantir que é a nova
    default_args=default_args,
    description="Pipeline Kdrama (Bronze, Silver, Gold) Docker Final",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["kdrama", "final_docker"],
) as dag:

    run_bronze_pipeline = BashOperator(
        task_id="run_bronze_final",
        bash_command=(
            # O script python já está no src_project/pipelines, então o python pode ser chamado assim
            # e a lógica de PROJECT_ROOT dentro do script (../../) deve funcionar para achar data_project
            # se o diretório de trabalho base for /opt/airflow (ou src_project)
            f"export PYTHONPATH=$PYTHONPATH:{PROJECT_SRC_IN_CONTAINER}; " # Garante que 'common' seja encontrável
            f"cd {PROJECT_SRC_IN_CONTAINER} && " # Muda para o diretório dos scripts
            f"{PYTHON_EXECUTABLE} {BRONZE_SCRIPT_RELATIVE_PATH}"
        )
    )

    run_silver_pipeline = BashOperator(
        task_id="run_silver_final",
        bash_command=(
            f"export PYTHONPATH=$PYTHONPATH:{PROJECT_SRC_IN_CONTAINER}; "
            f"cd {PROJECT_SRC_IN_CONTAINER} && "
            f"{PYTHON_EXECUTABLE} {SILVER_SCRIPT_RELATIVE_PATH}"
        )
    )

    run_gold_pipeline = BashOperator(
        task_id="run_gold_final",
        bash_command=(
            f"export PYTHONPATH=$PYTHONPATH:{PROJECT_SRC_IN_CONTAINER}; "
            f"cd {PROJECT_SRC_IN_CONTAINER} && "
            f"{PYTHON_EXECUTABLE} {GOLD_SCRIPT_RELATIVE_PATH}"
        )
    )

    run_bronze_pipeline >> run_silver_pipeline >> run_gold_pipeline