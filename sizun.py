from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'witold',
    'depends_on_past': False,
    'email': ['wtoldwrob@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': days_ago(1),
}
dag = DAG(
    'sizun',
    default_args=default_args,
    description='Sizun DAG',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=[],
)
t_ouest_france = BashOperator(
    task_id='ouest_france',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py ouest_france',
    dag=dag,
    trigger_rule='all_done'
)

t_immonot = BashOperator(
    task_id='immonot',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py immonot',
    dag=dag,
    trigger_rule='all_done'
)


t_figaro = BashOperator(
    task_id='figaro',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py figaro',
    dag=dag,
    trigger_rule='all_done'
)

t_superimmo = BashOperator(
    task_id='superimmo',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py superimmo',
    dag=dag,
    trigger_rule='all_done'
)

t_planete_immobilier = BashOperator(
    task_id='planete_immobilier',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py planete_immobilier',
    dag=dag,
    trigger_rule='all_done'
)

t_audierne_immobilier = BashOperator(
    task_id='audierne_immobilier',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py audierne_immobilier',
    dag=dag,
    trigger_rule='all_done'
)

t_bourse_immobilier = BashOperator(
    task_id='bourse_immobilier',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py bourse_immobilier',
    dag=dag,
    trigger_rule='all_done'
)

t_finistere_immobilier = BashOperator(
    task_id='finistere_immobilier',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py finistere_immobilier',
    dag=dag,
    trigger_rule='all_done'
)

t_bretagne_immobilier = BashOperator(
    task_id='bretagne_immobilier',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py bretagne_immobilier',
    dag=dag,
    trigger_rule='all_done'
)

t_immoplus29 = BashOperator(
    task_id='immoplus29',
    bash_command='/root/sizun/venv/bin/python /root/sizun/scrapper.py immoplus29',
    dag=dag,
    trigger_rule='all_done'
)

t_check_and_build = BashOperator(
    task_id='check_and_build',
    bash_command='/root/sizun/venv/bin/python /root/sizun/check_changes.py "{{dag_run.start_date}}"',
    dag=dag,
    trigger_rule='all_done'
)


t_ouest_france >> t_immonot >> t_figaro >> t_superimmo >>  t_planete_immobilier >> t_audierne_immobilier >> t_bourse_immobilier >> t_finistere_immobilier >> t_bretagne_immobilier >> t_immoplus29 >> t_check_and_build