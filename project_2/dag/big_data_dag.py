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
path = os.path.expanduser('~/airflow/dags/big-data/')
sys.path.append(path)

test_flag = True

input_file = 'hdfs:///sensor_data.txt'
output_folder = 'hdfs:///results/'
cd = 'cd ${AIRFLOW_HOME}; cd airflow/dags/big-data/scripts;'

if test_flag==True:
    input_file = f'{path}/test_data/input/2022-05_bmp180.csv'
    output_folder = f'{path}/test_data/output/'

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

    startHdfs = BashOperator(
    task_id='startHdfs',
    bash_command=cd+'bash start_hdfs.sh'
    ) 

    downloadFile = BashOperator(
    task_id='downloadFile',
    bash_command=cd+f'bash download.sh {date.today().year-1} {date.today().month-1:02d}'
    )

    extractPushFile = BashOperator(
    task_id='extractPushFile',
    bash_command=cd+f'bash extract_push.sh {input_file}'
    )

    printResults = BashOperator(
    task_id='printResults',
    bash_command=cd+f'bash results.sh'
    )

    # dependencies
    startHdfs >> downloadFile >> extractPushFile >> printResults
