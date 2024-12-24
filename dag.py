from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta

dag_id = 'daily_data_collection_final_DAG'
start_date = datetime(2023, 1, 1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval='0 4 * * *',  # Запуск каждый день в 4:00
    catchup=True,
    max_active_tasks=1,
    max_active_runs=1,
)

# Получаем переменные из Airflow
v_api_key = Variable.get('api_key')
v_api_url = Variable.get('api_url')

# Получаем соединение из Airflow
connection = BaseHook.get_connection('postgres_main_conn')

# Задача для запуска скрипта
run_script = BashOperator(
    task_id='run_script',
    bash_command=(
        f'python3 /airflow/scripts/dag/main_script.py ' +
        f'--api_url "{v_api_url}" ' +
        f'--api_key "{v_api_key}" ' +
        f'--query_date "{{{{ (ds | ds_add(-1)) }}}}" ' +
        f'--host {connection.host} ' +
        f'--dbname {connection.schema} ' +
        f'--user {connection.login} ' +
        f'--jdbc_password {connection.password} ' +
        f'--port {connection.port}'
    ),
    dag=dag,
)