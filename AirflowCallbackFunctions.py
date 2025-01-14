"""
Dummy DAG to demonstrate callback functions.
Details -  

"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define the execute callback function
def execute_callback(context):
    task_id = context['task_instance_key_str']
    print(f"Task {task_id} has started execution.")
    # Custom logic like logging or initializing resources

# Define the success callback function
def success_callback(context):
    task_id = context['task_instance_key_str']
    print(f"Task {task_id} succeeded!")
    # todo on task's success

# Define the failure callback function
def failure_callback_func_1(context):
    task_id = context['task_instance_key_str']
    error = context['exception']
    print(f"Task {task_id} failed with error: {error}")
    # todo on task's failure

# Define the failure callback function
def failure_callback_func_2(context):
    task_id = context['task_instance_key_str']
    error = context['exception']
    print(f"Task {task_id} failed with error: {error}")
    # Example: Send a failure alert
    send_email('user@abc.com', f"Task {task_id} failed!")

# Define the retry callback function
def retry_callback(context):
    task_id = context['task_instance_key_str']
    retry_count = context['ti'].try_number
    print(f"Task {task_id} is being retried. Attempt #{retry_count}")
    # Custom logic like logging the retry or sending a notification

# Define the SLA miss callback function
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print("SLA miss detected!")
    print(f"Dag: {dag.dag_id}")
    print(f"Tasks: {task_list}")
    print(f"SLAs: {slas}")
    # Custom logic such as sending alerts or logging the miss


# Define a sample Python callable
def process_data():
    print("Task is running...")

# Define the DAG
dag = DAG(
    'example_on_execute',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
    },
    schedule_interval='@daily',
    on_retry_callback=retry_callback, # applies to all tasks within DAG
    sla_miss_callback=sla_miss_callback, # applies to all tasks within DAG if SLA is defined for task
)

# Define the task1 without SLA
task = PythonOperator(
    task_id='start_task',
    python_callable=process_data,
    on_execute_callback=execute_callback, # triggers when task starts
    on_success_callback=success_callback, # triggers when task completes without error
    on_failure_callback=[failure_callback_func_1, failure_callback_func_2], # invokes both faliure callback functions 
    dag=dag,
)

# Define the task2 with SLA
task = PythonOperator(
    task_id='data_processing',
    python_callable=process_data,
    on_failure_callback=failure_callback_func_1, # invokes first failure callback function
    sla=timedelta(seconds=300), # SLA of 5 mins, if task doesn't completes within 5 mins, sla_miss_callback will be invoked
    dag=dag,
)
