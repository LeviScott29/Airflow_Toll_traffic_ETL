from datetime import timedelta
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#define dag argument
default_args = {
    'owner': 'levi scott',
    'start_date': days_ago(0),
    'email' : ['someone@mail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes =5),
}

#define DAG
dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days =1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

#unzip data
unzip_data= BashOperator(
    task_id="unzip_data",
    bash_command='tar -zxvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)

#extract data from csv
extract_data_from_csv=BashOperator(
    task_id='extract_dat_from_csv',
    bash_command="cut -d ',' -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv",
    dag=dag,
)

#extract data from tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | tr "\t" "," > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

#extract from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id='Extract_data_from_fixed_width',
    bash_command='cut -c 59-67 /home/project/airflow/dags/finalassignment/payment-data.txt | sed "s/ /,/g" > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)

#consolidate data
consolidate_data = BashOperator(
    task_id='Consolidate_data',
    bash_command='paste -d "," /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag,
)


#define pipeline
unzip_data >> extract_data_from_csv 
extract_data_from_csv >> extract_data_from_tsv 
extract_data_from_tsv >> extract_data_from_fixed_width
extract_data_from_fixed_width >> consolidate_data 
consolidate_data
