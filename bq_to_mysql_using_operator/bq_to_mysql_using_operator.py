###############################################################################################
#       * Transfer BQ table data to mysql (on-premise) *                                      #
###############################################################################################

from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime,timedelta, date
from airflow.utils.dates import days_ago
import yaml
import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.transfers.bigquery_to_mysql import BigQueryToMySqlOperator

datestring = datetime.datetime.now().strftime("%Y%m%d")


DAG_Name = 'bq_to_mysql_using_operator' 

with open('/home/airflow/gcs/dags/{}/configs/config_{}.yml'.format(DAG_Name, DAG_Name)) as master_yaml_file:
    parsed_master_yaml_file = yaml.load(master_yaml_file,Loader=yaml.Loader)

dataset_table = parsed_master_yaml_file['dataset_table']
database = parsed_master_yaml_file['database']
mysql_table_name = parsed_master_yaml_file['mysql_table_name']

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1
    #'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_Name,
          default_args=default_args,
          schedule_interval=None, # in UTC
          catchup=False,
          max_active_runs=1,
          template_searchpath=['/home/airflow/gcs/dags/']
          )

START_TASK = DummyOperator(task_id="START_TASK",
                           dag=dag)
        
#Import BQ to mysql         
transfer_data = BigQueryToMySqlOperator(
            task_id='transfer_data',
            gcp_conn_id='project_bq_conn_del_ce',
            dataset_table=dataset_table,      #BQ dataset and table name
            mysql_conn_id = "mysql_conn",     #mysql connection 
            database=database,      #mysql database
            mysql_table=mysql_table_name,    #mysql table name
            replace=True,
            dag=dag
        )
        
START_TASK >> transfer_data