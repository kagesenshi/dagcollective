from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.sqoop_operator import SqoopOperator
import os

# TABLES=[{
#     'name': 'table1',
#     'split_by': 'created',
#     'mappers': 1,
#     'direct': False, 
#     'hive-database': None,
#     'hive-table': None,
#     'format': 'parquet',
#     'format-options': None,
#     'partition_fields': ['field1'], # future, CTAS does not support partition
#     'bucket_fields': ['field2'] # future, CTAS does not support buckets
# }]

# dag = DAG(JOB_NAME, default_args=default_args, schedule_interval=timedelta(days=1))

def hook(dag, conn_id, tables, staging_dir='/tmp/airflow', staging_db=None, **options):

    staging_db = staging_db or 'staging_%s' % conn_id

    create_staging_db = HiveOperator(
        task_id='create_staging_db',
        hql='create database if not exists %s;' % staging_db,
        dag=dag
    )

    create_staging_dir = BashOperator(
        task_id='create_staging_dir',
        bash_command='hdfs dfs -mkdir -p %s' % staging_dir,
        dag=dag
    )

    for tbl in tables:
        table = {
                'hive-database': None,
                'hive-table': None,
                'mappers': 1,
                'direct': False,
                'format': 'parquet',
                'format-options': None,
                'partition_fields': [],
                'bucket_fields': []
        }
        table.update(tbl)
        assert table['hive-database'] is not None
        if table['hive-table'] is None:
            table['hive-table'] = table['name']

            
        staging_tbl_dir = os.path.join(staging_dir, conn_id, table['name'])

        clean_sqoop_staging = BashOperator(
            task_id=('clean_sqoop_staging_dir.%s' % (table['name'])).lower(),
            bash_command='hdfs dfs -rm -R -f %s' % staging_tbl_dir,
            dag=dag)
        
        
        clean_staging_tbl = HiveOperator(
            task_id=('clean_staging_table.%s' % (table['name'])).lower(),
            hql='''drop table if exists %(staging_db)s.%(staging_tbl)s''' % {
               'staging_db': staging_db,
               'staging_tbl': table['name']
            }, dag=dag)
        
        sqoop = SqoopOperator(
            task_id=('sqoop.%s' % (table['name'])).lower(),
            conn_id=conn_id,
            table=table['name'],
            split_by=table['split_by'],
            num_mappers=table['mappers'],
            direct=table['direct'],
            target_dir=staging_tbl_dir,
            extra_import_options={
               'hive-import': '',
               'hive-database': staging_db,
               'hive-table': table['name'],
               'hive-delims-replacement': ' ',
               'temporary-rootdir': staging_dir,
            },
            dag=dag)
        

        create_statement = ('create table %s.%s_tmp\n') % (
                    table['hive-database'],
                    table['hive-table'])

        create_statement += 'stored as %s\n' % table['format']

        format_opts = table.get('format-options', None)
        if format_opts:
            create_statement += '%s\n' % format_opts

        convert_to_parquet = HiveOperator(
            task_id=('hive_convert_format.%s' % (table['name'])).lower(),
            hql=(
             'create database if not exists %(dst_db)s;\n'
             'drop table if exists %(dst_db)s.%(dst_tbl)s_tmp;\n'
             '%(create_statement)s'
             'as select * from %(staging_db)s.%(staging_tbl)s;\n'
             'drop table if exists %(dst_db)s.%(dst_tbl)s;\n'
             'alter table %(dst_db)s.%(dst_tbl)s_tmp rename to  %(dst_db)s.%(dst_tbl)s;\n'
             ) % {
                'dst_db': table['hive-database'],
                'dst_tbl': table['hive-table'],
                'staging_db': staging_db,
                'staging_tbl': table['name'],
                'create_statement': create_statement
            }, dag=dag)
      
        clean_staging_tbl.set_upstream(create_staging_db)
        clean_sqoop_staging.set_upstream(create_staging_dir)
        sqoop.set_upstream(clean_sqoop_staging)
        sqoop.set_upstream(clean_staging_tbl)
        convert_to_parquet.set_upstream(sqoop)
        
