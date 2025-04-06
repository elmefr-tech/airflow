from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import logging
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from airflow.models.variable import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import logging



###
#
def log_dummy_task(context):
    logger.info("EMO: Exécution de DummyOperator: "+context['task_instance_key_str'])

###
@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    raise AirflowException("Failing task because one or more upstream tasks failed.")
###
logger = logging.getLogger('airflow.task')
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('mon_dag',
        default_args=default_args,
        schedule_interval=None,
        tags=['EMO Test Tasks']) as dag:

    # Définition des tâches
    tache1 = DummyOperator(task_id='tache1', dag=dag, on_success_callback=log_dummy_task,)
    tache2 = DummyOperator(task_id='tache2', dag=dag, on_success_callback=log_dummy_task,) 
    tache3 = DummyOperator(task_id='tache3', dag=dag, on_success_callback=log_dummy_task,)
    tache4 = DummyOperator(task_id='tache4', dag=dag, on_success_callback=log_dummy_task,)

    #
    tache1_1 = DummyOperator(task_id='tache1.1', dag=dag, on_success_callback=log_dummy_task,)
    tache1_2 = DummyOperator(task_id='tache1.2', dag=dag, on_success_callback=log_dummy_task,)
    tache1_3 = DummyOperator(task_id='tache1.3', dag=dag, on_success_callback=log_dummy_task,)
    tache1_4 = DummyOperator(task_id='tache1.4', dag=dag, on_success_callback=log_dummy_task,)

    # Configuration des dépendances
    # tache1 >> tache1_1 >> tache3 >> tache1_3  # tache3 dépend de tache1
    # tache2 >> tache1_2 >> tache3 >> tache1_3  # tache3 dépend de tache2
    # tache1 >> tache1_1 >> tache4 >> tache1_4  # tache4 dépend de tache1

    # Alternative syntaxe plus concise :
    # [tache1>> tache2 , tache1_1 >> tache1_2] >> tache3 >> tache1_3
    # [tache1 >> tache1_1, tache4] >> tache1_4

    tache1 >> tache3 >> tache1_3
    tache2 >> tache3 >> tache1_3
    tache1 >> tache4 >> tache1_4
    tache1 >> tache1_1
    tache2 >> tache1_2

    list(dag.tasks) >> watcher()