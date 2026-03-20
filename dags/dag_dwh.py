"""
This DAG orchestrates the entire data pipeline (ELT) for the Loan Risk Data Warehouse project, including:
1. Extracting and Loading data from CSV files in Google Cloud Storage to the Bronze stage in BigQuery.
2. Transforming data from the Bronze stage to the Silver stage using Python and Pandas.
3. Transforming data from the Silver stage to the Gold stage using Python.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from datetime import datetime

# Import the individual transform functions from my modularized script
import sys
sys.path.append('/home/minhthanh2004kid/airflow/dags/pipeline')
from transform_to_silver import (
    transform_application,
    transform_bureau,
    transform_bureau_balance,
    transform_credit_card_balance,
    transform_pos_cash_balance,
    transform_installments_payments,
    transform_previous_application
)

with DAG(
    dag_id="dwh_pipeline",
    start_date=datetime(2026, 3, 20),
    schedule_interval='@daily',
    # execution_timeout = timedelta(minutes=30),
    catchup=False,
    description='Run full pipeline from GCS to BigQuery Bronze, Silver, and Gold for Home Credit data',
    tags=['home_credit', 'bronze', 'silver', 'gold', 'Data Warehouse'],
) as dag:

    # 1. Extract to Bronze
    extract = BashOperator(
        task_id="extract_data",
        bash_command="python /home/minhthanh2004kid/airflow/dags/pipeline/el_to_bronze.py"
    )

    # 2. Parallel Transform Tasks (Bronze to Silver)
    @task(task_id="transform_application_silver")
    def task_transform_application():
        transform_application()

    @task(task_id="transform_bureau_silver")
    def task_transform_bureau():
        transform_bureau()

    @task(task_id="transform_bureau_balance_silver")
    def task_transform_bureau_balance():
        transform_bureau_balance()

    @task(task_id="transform_cc_balance_silver")
    def task_transform_cc_balance():
        transform_credit_card_balance()

    @task(task_id="transform_pos_cash_silver")
    def task_transform_pos_cash():
        transform_pos_cash_balance()

    @task(task_id="transform_installments_silver")
    def task_transform_installments():
        transform_installments_payments()

    @task(task_id="transform_previous_app_silver")
    def task_transform_previous_app():
        transform_previous_application()

    # Instantiate the tasks
    t_app = task_transform_application()
    t_bur = task_transform_bureau()
    t_bur_bal = task_transform_bureau_balance()
    t_cc_bal = task_transform_cc_balance()
    t_pos = task_transform_pos_cash()
    t_inst = task_transform_installments()
    t_prev = task_transform_previous_app()

    # 3. Load to Gold
    # load_gold = BashOperator(
    #     task_id="transform_data_to_gold",
    #     bash_command="python /home/minhthanh2004kid/airflow/dags/pipeline/transform_to_gold.py"
    # )

    # 4. Set Dependencies
    # Extract runs first.
    # Then ALL silver transformations run in parallel.
    # Gold transformation waits for ALL silver tasks to finish successfully.
    extract >> [t_app, t_bur, t_bur_bal, t_cc_bal, t_pos, t_inst, t_prev] # >> load_gold