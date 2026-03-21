"""
This DAG orchestrates the entire data pipeline (ELT) for the Loan Risk Data Warehouse project.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from datetime import datetime

import sys
sys.path.append('/home/minhthanh2004kid/airflow/dags/pipeline')

from transform_to_silver import (
    transform_application as silver_app,
    transform_bureau as silver_bur,
    transform_bureau_balance as silver_bur_bal,
    transform_credit_card_balance as silver_cc_bal,
    transform_pos_cash_balance as silver_pos,
    transform_installments_payments as silver_inst,
    transform_previous_application as silver_prev
)
from transform_to_gold import (
    transform_application as gold_app,
    transform_bureau as gold_bur,
    transform_credit_card_balance as gold_cc_bal,
    transform_installments_payments as gold_inst,
    transform_previous_applications as gold_prev,
    transform_pos_cash_balance as gold_pos
)

with DAG(
    dag_id="dwh_pipeline_parallel",
    start_date=datetime(2026, 3, 22),
    schedule_interval=None,
    catchup=False,
    description='Run full parallel pipeline for Home Credit data',
    tags=['home_credit', 'bronze', 'silver', 'gold', 'Data Warehouse'],
) as dag:

    # STAGE 1
    extract = BashOperator(
        task_id="extract_data_to_bronze",
        bash_command="python /home/minhthanh2004kid/airflow/dags/pipeline/el_to_bronze.py"
    )

    # STAGE 2
    @task(task_id="transform_application_silver")
    def task_transform_application_silver(): return silver_app()

    @task(task_id="transform_bureau_silver")
    def task_transform_bureau_silver(): return silver_bur()

    @task(task_id="transform_bureau_balance_silver")
    def task_transform_bureau_balance_silver(): return silver_bur_bal()

    @task(task_id="transform_cc_balance_silver")
    def task_transform_cc_balance_silver(): return silver_cc_bal()

    @task(task_id="transform_pos_cash_silver")
    def task_transform_pos_cash_silver(): return silver_pos()

    @task(task_id="transform_installments_silver")
    def task_transform_installments_silver(): return silver_inst()

    @task(task_id="transform_previous_app_silver")
    def task_transform_previous_app_silver(): return silver_prev()

    # Group Silver tasks into a list
    silver_tasks = [
        task_transform_application_silver(),
        task_transform_bureau_silver(),
        task_transform_bureau_balance_silver(),
        task_transform_cc_balance_silver(),
        task_transform_pos_cash_silver(),
        task_transform_installments_silver(),
        task_transform_previous_app_silver()
    ]

    # CHECKPOINT: Wait for ALL Silver tasks to finish
    wait_for_silver_stage = EmptyOperator(task_id="WAIT_FOR_SILVER_STAGE")

    # STAGE 3
    @task(task_id="transform_application_gold")
    def task_transform_app_gold(): return gold_app()

    @task(task_id="transform_bureau_gold")
    def task_transform_bureau_gold(): return gold_bur()

    @task(task_id="transform_credit_card_balance_gold")
    def task_transform_ccb_gold(): return gold_cc_bal()

    @task(task_id="transform_installments_payments_gold")
    def task_transform_ip_gold(): return gold_inst()

    @task(task_id="transform_previous_applications_gold")
    def task_transform_pa_gold(): return gold_prev()

    @task(task_id="transform_pos_cash_balance_gold")
    def task_transform_poscash_gold(): return gold_pos()

    # Group Gold tasks into a list
    gold_tasks = [
        task_transform_app_gold(),
        task_transform_bureau_gold(),
        task_transform_ccb_gold(),
        task_transform_ip_gold(),
        task_transform_pa_gold(),
        task_transform_poscash_gold()
    ]

    # CHECKPOINT: Pipeline Complete
    pipeline_complete = EmptyOperator(task_id="PIPELINE_COMPLETE")

    # RUN DEPEND
    # 1. Extract runs first, then triggers all Silver tasks simultaneously
    extract >> silver_tasks 
    
    # 2. ALL Silver tasks must finish before hitting the "Wait" checkpoint
    silver_tasks >> wait_for_silver_stage
    
    # 3. Once Silver is 100% done, trigger all Gold tasks simultaneously
    wait_for_silver_stage >> gold_tasks
    
    # 4. Mark pipeline as complete once all Gold tasks finish
    gold_tasks >> pipeline_complete