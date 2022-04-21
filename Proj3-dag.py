from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.contrib.operators.ssh_operator import SSHOperator


# @dag(
#     schedule_interval="0 0 * * *",
#     start_date=pendulum.datetime(2022, 4, 17, tz="UTC"),
#     catchup=False,
#     dagrun_timeout=datetime.timedelta(minutes=60),
# )
default_args = {
    "owner": "tty",
    "start_date": datetime(2022, 4, 19)
}

dag = DAG("Analyze",
        description="Analyze LE",
        default_args=default_args,
        schedule_interval='0 8 * * *')

script = 'hdfs://localhost:LE/Script/spark.py'
spark_parameters = '--executor-memory 100G'
# here we can use Airflow template to define the parameters used in the script
# parameters = '--db {{ params.database_instance }}, --output_path {{ params.output_path }}' 

submit_pyspark_job = SSHOperator(
    task_id='pyspark_submit',
    ssh_conn_id='LE spark',
    command='set -a; PYSPARK_PYTHON=python3; /usr/bin/spark-submit --deploy-mode cluster %s %s' % (spark_parameters, script),
    dag=dag
)
#     submit_task

# dag = Analyze()
