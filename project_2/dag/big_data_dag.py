from datetime import date, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

import os
import sys

# Adding the folder to path
path = os.path.expanduser('~/airflow/dags/big-data/')
sys.path.append(path)

# define cd for later
cd = 'cd ${AIRFLOW_HOME}; cd airflow/dags/big-data/scripts;'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

# instantiate DAG
with DAG(
    'big_data_dag',
    default_args=default_args,
    start_date = datetime(2022,1,1),
    # scheduled 3rd of the month at 3:30 AM
    schedule_interval = '30 3 3 * *',
    tags=['big-data'],
    catchup=False,
) as dag:

    # starts hdfs. if already running it goes on
    startHdfs = BashOperator(
    task_id='startHdfs',
    bash_command=cd+'bash start_hdfs.sh'
    ) 
    
    # download file. pass current year and past month as parameters
    downloadFile = BashOperator(
    task_id='downloadFile',
    bash_command=cd+f'bash download.sh {date.today().year} {date.today().month-1:02d}'
    )

    # extract, push file to hdfs
    extractPushFile = BashOperator(
    task_id='extractPushFile',
    bash_command=cd+'bash extract_push.sh'
    )

    # run spark script
    runSpark = BashOperator(
    task_id='runSpark',
    bash_command=cd+'bash run_spark.sh'
    )

    # output results
    printResults = BashOperator(
    task_id='printResults',
    bash_command=cd+'bash results.sh'
    )

    # dependencies
    startHdfs >> downloadFile >> extractPushFile >> runSpark >> printResults
