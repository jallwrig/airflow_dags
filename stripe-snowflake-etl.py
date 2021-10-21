from logging import log
from multiprocessing import log_to_stderr
from typing import TYPE_CHECKING
from airflow import DAG 
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta, date
import json

STRIPE_CONN_ID = 'stripe_conn'
SQL_DELETE_ALL_ROWS_STATEMENT = "DELETE FROM CUSTOMERS"

def _transform_data(ti):
    stripe_json_str = ti.xcom_pull(key='return_value', task_ids=['stripe_get'])
    stripe_json = json.loads(stripe_json_str[0])
    stripe_data=stripe_json["data"]
    snowflake_insert_sql = "INSERT INTO CUSTOMERS (CUSTOMERID, NAME, EMAIL, ZIPCODE, ACCOUNTBALANCE, DATEUPLOADED) VALUES "
    for customer in stripe_data:
        snowflake_insert_sql += "('" + str(customer.get('id',"None")) + "','"
        snowflake_insert_sql += str(customer.get('name',"None")) + "','"
        snowflake_insert_sql += str(customer.get('email','None')) + "','" 
        sources_data = customer.get('sources')['data']
        if sources_data: 
            sources_data_json = sources_data[0]
            zipcode = str(sources_data_json.get('address_zip',"None"))
        else:
            zipcode = "None"
        snowflake_insert_sql += zipcode + "','"
        snowflake_insert_sql += str(customer.get('account_balance',0)) + "','" + date.today().strftime("%m/%d/%y") + "'),"
    snowflake_insert_sql = snowflake_insert_sql[0:-1] + ";"
    return snowflake_insert_sql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
 }

dag=DAG(
    'test-https-stripe-dag',
    schedule_interval=None,
    default_args=default_args,
    start_date=datetime.today() - timedelta(days=2),
    catchup=True,
    tags=['Stripe-Snowflake ETL'],
) 
    
stripe_get = SimpleHttpOperator(
    task_id='stripe_get',
    method='GET',
    http_conn_id='stripe_api',
    endpoint='/v1/customers',
    log_response = True,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable = _transform_data,
)

snowflake_delete_all = SnowflakeOperator(
    task_id='snowflake_delete_all',
    sql=SQL_DELETE_ALL_ROWS_STATEMENT,
    dag=dag,
)

snowflake_insert = SnowflakeOperator(
    task_id='snowflake_insert',
    sql="{{ ti.xcom_pull( task_ids=['transform_data'])[0] }}",
    dag=dag,
)

stripe_get  >> transform_data >> snowflake_delete_all >> snowflake_insert
