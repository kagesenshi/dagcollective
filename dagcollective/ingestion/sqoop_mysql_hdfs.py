from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.sqoop_operator import SqoopOperator
import os

CONN_ID='sqoop_myipcs_dissana'
SRC_TBL='frozen_frame_mspr'
DST_DB='myipcs_dissana_raw'
DST_TBL=SRC_TBL
SPLIT_COLUMN='created_at'
MAPPERS=4
NOTIFICATION_EMAIL='izhar@abyres.net'
START_DATE=datetime(2019, 7, 9)
SCHEDULE='0 1 * * *'


SRC_DB='cdm'
JOB_NAME=('sqoop.%s.%s' % (SRC_DB,SRC_TBL)).lower()
STAGING_DB='ingest_staging'
STAGING_TBL='STAGING_' + SRC_TBL
STAGING_ROOT='/user/airflow/sqoop-staging/%s' % SRC_DB
STAGING_TBL_DIR='%s/%s' % (STAGING_ROOT, SRC_TBL)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email': [NOTIFICATION_EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(JOB_NAME, default_args=default_args, schedule_interval=timedelta(days=1))

clean_sqoop_staging = BashOperator(
    task_id='clean_sqoop_staging_dir',
    bash_command='hdfs dfs -rm -R -f %s' % STAGING_TBL_DIR,
    dag=dag)


clean_staging_tbl = HiveOperator(
    task_id='clean_staging_table',
    hql='''drop table if exists %(staging_db)s.%(staging_tbl)s''' % {
       'staging_db': STAGING_DB,
       'staging_tbl': STAGING_TBL
    }, dag=dag)

sqoop = SqoopOperator(
    task_id='sqoop',
    conn_id='sqoop_myipcs_cdm',
    table=SRC_TBL,
    split_by=SPLIT_COLUMN,
    num_mappers=MAPPERS,
    target_dir=STAGING_TBL_DIR,
    extra_import_options={
       'hive-import': '',
       'hive-database': STAGING_DB,
       'hive-table': STAGING_TBL,
       'hive-delims-replacement': ' ',
       'temporary-rootdir': STAGING_ROOT,
    },
    dag=dag)

hive = HiveOperator(
    task_id='hive_convert_to_parquet',
    hql='''
         drop table if exists %(dst_db)s.%(dst_tbl)s_tmp;
         create table %(dst_db)s.%(dst_tbl)s_tmp stored as parquet
         as select * from %(staging_db)s.%(staging_tbl)s;
         drop table if exists %(dst_db)s.%(dst_tbl)s;
         alter table %(dst_db)s.%(dst_tbl)s_tmp RENAME TO %(dst_db)s.%(dst_tbl)s;
    ''' % {
        'dst_db': DST_DB,
        'dst_tbl': DST_TBL,
        'staging_db': STAGING_DB,
        'staging_tbl': STAGING_TBL
    }, dag=dag)

sqoop.set_upstream(clean_sqoop_staging)
sqoop.set_upstream(clean_staging_tbl)
hive.set_upstream(sqoop)

