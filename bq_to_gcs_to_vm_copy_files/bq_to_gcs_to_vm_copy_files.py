#**********Dag pipeline code to import historical and incremental data from Bigquery to Postgre on CloudSQL*********
#Steps: 1. If load type is 'hist', Export whole data from bigquery to gcs files in csv
#       2. If load type is 'incr', export latest data based on date data from bigquery to gcs files in csv
#       3. List exported csv file names, concat file names and generate gsutil command to merge multiple csv files into one
#       4. if load type is 'hist', import merged file into postgre target table
#       5. if load type is 'incr', import merged file into postgre stage table and then upsert into target from stage.



from airflow.operators.dummy_operator import DummyOperator 
from airflow.models import DAG 
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator 
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator 
from airflow.operators import bash_operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator
from airflow.utils.trigger_rule import TriggerRule
import yaml
import datetime



datestring = datetime.datetime.now().strftime("%Y%m%d")
DAG_Name = 'bq_to_gcs_to_vm_copy_files' 


#Reading configuaration YML file in a dictionary
with open('/home/airflow/gcs/dags/{}/configs/config_{}.yml'.format(DAG_Name, DAG_Name)) as master_yaml_file:
    parsed_master_yaml_file = yaml.load(master_yaml_file,Loader=yaml.Loader)



common_args=parsed_master_yaml_file['args']
del parsed_master_yaml_file['args']


bucket_name=common_args['bucket_name']
composer_bucket=common_args['composer_bucket']
export_file_name=common_args['export_file_name']
merged_file_name=common_args['merged_file_name']
cloudsql_instance=common_args['cloudsql_instance']
cloudsql_importuser=common_args['cloudsql_importuser']
location=common_args['location']
vm_instance_user= common_args['vm_instance_user']
vm_instance_zone = common_args['vm_instance_zone']
mount_directory = common_args['mount_directory']



#function to generate gsutil command to merge multiple CSV files into single file.
def listFileGroups(bucket_name, bq_table, datestring, **kwargs):
        xcom_task_id=kwargs["xcom_task_id"]
        filelist=kwargs['task_instance'].xcom_pull(task_ids=xcom_task_id)
        cnt=len(filelist)
        a=1
        t=-1
        dictfilelist={}
        grp =(cnt/31)+1
        while a<=grp:
            b=0
            list_a=[]
            while b<31:
                if t<len(filelist)-1:
                    t=t+1
                else:
                    break
                list_a.insert(b, filelist[t])
                b=b+1
            dictfilelist.update({a:list_a})
            a=a+1
        firstfile=''
        commandfinal=''


        bucket_prefix='gs://{}/'.format(bucket_name)
       
        merged_prefix_file=f'gs://{bucket_name}/{bq_table}/{datestring}/{merged_file_name}'
        for ditem in dictfilelist.values():
            command_compose='gsutil compose '+firstfile
            firstfile=' '
            for litem in ditem:
                
                command_compose=command_compose+firstfile+bucket_prefix +litem

            firstfile=merged_prefix_file
            commandfinal= commandfinal + command_compose +" "+ firstfile + ";"

        return commandfinal






default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 3
    #'retry_delay': timedelta(minutes=1)
}



dag = DAG(DAG_Name,
          default_args=default_args,
          schedule_interval=None, # in UTC
          catchup=False,
          max_active_runs=1,
          template_searchpath=['/home/airflow/gcs/dags/']
          )


  

# Loop to iterate over a dict of table names
for i in parsed_master_yaml_file.keys():

    table_config=parsed_master_yaml_file[i]
    project_name =table_config['project_name']
    dataset_name= table_config['dataset_name']
    bq_table =table_config['bq_table']
    cloudsql_database=table_config['cloudsql_database']
    cloudsql_table=table_config['cloudsql_table'] 
    cloudsql_staging_table=table_config['cloudsql_staging_table']
    type_load=table_config['type_load']
    
    
    cloudsql_project=project_name
    folder_name=f"{dataset_name}/{bq_table}/{datestring}"
  
    prefix=f"{folder_name}/"
    bq_to_gcs_sql= f'sql/EXPORT_{bq_table}_{type_load}.sql'
    export_file_name = export_file_name.split('.')[0]  
    
    
    START_TASK = DummyOperator(task_id=f"START_TASK_{bq_table}",
               dag=dag)


    #Exporting BQ table to GCS folder in CSV file format.
    BIGQUERY_TO_GCS = BigQueryOperator(
                task_id=f"BIGQUERY_TO_GCS_{bq_table}",
                sql=bq_to_gcs_sql,
                use_legacy_sql=False,
                location=location,
                dag=dag,
                params={"project_name":project_name,
                "bucket_name": bucket_name,
                "folder_name": folder_name,
                "file_name":export_file_name}
            )


    #Creating a list of file names present in GCS folder
    STORAGE_LIST_FILES=GCSListObjectsOperator(
            task_id=f"STORAGE_LIST_FILES_{bq_table}",
            bucket=bucket_name,
            prefix=prefix,
            delimiter='.csv',
            dag=dag)

    xcom_task_id=f"STORAGE_LIST_FILES_{bq_table}"
    
    
    #Generating command to merge multiple CSV files into single file
    FILES_CONCAT = PythonOperator(
            task_id=f'FILES_CONCAT_{bq_table}',
            provide_context=True,
            python_callable=listFileGroups, 
            op_kwargs={
                'xcom_task_id': xcom_task_id, 
                'merged_file_name' :merged_file_name,
                "bucket_name":bucket_name,
                "bq_table": bq_table,
                "datestring": datestring},
            dag=dag)

    xcom_task_id=f'FILES_CONCAT_{bq_table}'
    
    #Executing generated command from previous step to merge files
    EXECUTE_MERGE_FILES =bash_operator.BashOperator(
        task_id=f'EXECUTE_MERGE_FILES_{bq_table}',
        bash_command="{{task_instance.xcom_pull(task_ids='" + xcom_task_id +"')}}",
        dag=dag)

    
    merged_prefix_file=f'gs://{bucket_name}/{bq_table}/{datestring}'
    
    source_file = merged_prefix_file
    vm_instance_user = vm_instance_user
    vm_instance_zone = vm_instance_zone
    projectid = project_name  #'dmgcp-del-ce'
    mount_directory = mount_directory
    table_name = bq_table
    
    cmd = """gcloud compute ssh {} --zone "{}"  --tunnel-through-iap  --project "{}" --command "mkdir -p {}/{}; gsutil cp -r {} {}/{}/\"""".format(vm_instance_user, vm_instance_zone, projectid, mount_directory, bq_table, source_file, mount_directory, bq_table)
    
    move_file_to_vm =bash_operator.BashOperator(
        task_id=f'move_file_to_vm_{bq_table}',
        bash_command=cmd,
        dag=dag, 
        retries= 3
    )   
    
    
    
    END_TASK = DummyOperator(task_id=f"END_TASK_{bq_table}", 
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag
    )
    
    
    #Task dependancy
    START_TASK >> BIGQUERY_TO_GCS >> STORAGE_LIST_FILES >> FILES_CONCAT >>  EXECUTE_MERGE_FILES >> move_file_to_vm >> END_TASK