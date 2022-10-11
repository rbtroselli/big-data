### To solve "Hostname of job runner does not match", set "hostname_callable = socket.gethostname" in airflow.cfg.
# One dot, no column, otherwise it gets corrected... and the scheduler gets stuck until it restarts 
# (because of SequentialExecutor and sqlite, during the execution, the scheduler has no heartbeat and doesn't progress)
### If the scheduler gets stuck and the task stays is queued, check for errors or corrections in console (even example dags)
# https://stackoverflow.com/questions/57681573/how-to-fix-the-error-airflowexceptionhostname-of-job-runner-does-not-match
## to log and debug, change in airflow.cfg: log_filename_template = {{ ti.dag_id }}.log

from datetime import date, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

# Adding the folder to path
# ~ has to be expanded. works both on Macos and linux
import os
import sys
path = os.path.expanduser('~/airflow/dags/big-data/scripts/')
sys.path.append(path)

test_flag = True

source_file_path = 'hdfs:///sensor_data.txt'

if test_flag:
    source_file_path = os.curdir
    print(source_file_path)

def test_function(value):
    print(value)


# default_args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

# instantiate DAG
with DAG(
    'big_data_dag',
    default_args=default_args,
    start_date = datetime(2022,1,1), # best practice (Astronomer): define date in DAG
    # scheduled 3rd of the month at 3:30 AM
    schedule_interval = '30 3 3 * *',
    tags=['big-data'],
    catchup=False, # doesnt work in default args
) as dag:

    # # tasks
    # url_scrape = PythonOperator(
    #     task_id='url_scrape',
    #     python_callable=url_scraper,
    #     op_args=[path],
    # )

    dummy_operator_1 = DummyOperator(
        task_id='dummy_operator_1'
    )

    dummy_operator_2 = DummyOperator(
        task_id='dummy_operator_2'
    )
    
    testOperator = BashOperator(
    task_id='testOperator',
    bash_command='cd ${AIRFLOW_HOME}; cd airflow/dags/big-data/scripts; pwd '
    )

    downloadOperator = BashOperator(
    task_id='downloadOperator',
    bash_command='\
        cd ${AIRFLOW_HOME};\
        cd airflow/dags/big-data/scripts;'\
        +f'bash download.sh {date.today().year-1} {date.today().month-1:02d}'
    )

    # extractPushOperator = BashOperator(
    # task_id='extractPushOperator',
    # bash_command='\
    #     cd ${AIRFLOW_HOME};\
    #     cd airflow/dags/big-data/scripts;'\
    #     +f'bash extract_push.sh {source_file_path}'
    # )

    testOperator2 = PythonOperator(
        task_id='testOperator2',
        python_callable=test_function,
        op_args=[source_file_path]
    )

    # jobs_load = PythonOperator(
    #     task_id='jobs_load',
    #     python_callable=jobs_loader,
    #     op_args=[path],
    # )

    # jobs_merge = PythonOperator(
    #     task_id='jobs_merge',
    #     python_callable=jobs_merger,
    #     op_args=[path],
    # )

    # pipeline
    dummy_operator_1 >> testOperator >> downloadOperator >> testOperator2 >> dummy_operator_2
