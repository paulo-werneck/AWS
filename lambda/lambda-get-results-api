import boto3
from collections import namedtuple
import pandas as pd
from datetime import datetime
from s3fs import S3FileSystem
import fastparquet as fp

s3 = boto3.client('s3')
athena = boto3.client('athena')
s3_fs = S3FileSystem()
current_date = datetime.today().strftime('%Y%m%d')


def get_data_s3(bucket, key, date, region):
    """ Pega os dados gerados pelo evento (insercao de obj no S3) """

    key_event = key.split('/')
    id_folder_event = key_event[-2]
    path_event = '/'.join(key_event[:-1]) + '/'
    date_event = date[0:10].replace('-', '')
    file_event = key_event[-1]

    key_stage = key.replace('results', 'report/stage').split('/')
    path_stage = '/'.join(key_stage[:-2]) + '/'
    file_stage = f'results_api_{date_event}_{key_stage[-1]}'

    table_target = f'tb_{key_event[1]}_{file_event.split(".")[0]}'

    DataEvent = namedtuple('DataEvent', ['bucket', 'id_folder_event', 'path_event', 'file_event', 'date_event',
                                         'region_event', 'path_stage', 'file_stage', 'table_target'])

    data = DataEvent(bucket, id_folder_event, path_event, file_event, date_event, region,
                     path_stage, file_stage, table_target)
    return data


def join_stage_files(bucket, path_event, file_event, path_stage, file_stage, folder_id_event, data_event):
    """ Consolida os arquivos do evento em um unico arquivo na area de stage """

    key_stage = f'{path_stage}{file_stage}'
    result_obj = s3.list_objects(Bucket=bucket, Prefix=key_stage)

    if result_obj.get('Contents'):
        df_event = pd.read_csv(f's3://{bucket}/{path_event}{file_event}')
        df_event['id_transacao'] = folder_id_event
        df_event['dt_particao'] = data_event

        df = pd.read_csv(f's3://{bucket}/{key_stage}')
        df = df.append(df_event, ignore_index=True)
        df.to_csv(f's3://{bucket}/{key_stage}', index=False, index_label=False)
    else:
        s3.copy_object(
            Bucket=bucket,
            Key=key_stage,
            CopySource={'Bucket': bucket, 'Key': path_event + file_event},
            Metadata={'name': 'teste'},
            MetadataDirective='REPLACE'
        )
        df = pd.read_csv(f's3://{bucket}/{key_stage}')
        df.insert(0, 'id_transacao', folder_id_event)
        df['dt_particao'] = data_event
        df.to_csv(f's3://{bucket}/{key_stage}', index=False, index_label=False)


def load_target_from_stage(bucket, path_stage, table_name, database_name='turing_prd_scoreapi_usage'):
    """ Carrega os dados da area de stage para a area de target e converte para parquet """

    lst_files = s3_fs.glob(f's3://{bucket}/{path_stage}*')

    for file in lst_files:
        if current_date not in file:
            df = pd.read_csv(f's3://{file}')
            df.to_parquet(
                path=f's3://{bucket}/report/target/{database_name}/{table_name}/dt_particao={file.split("_")[-2]}/file.parquet.gzip',
                engine='fastparquet',
                compression='GZIP')


def load_athena_partitions(bucket, table_name, database_name='turing_prd_scoreapi_usage'):
    """ Carrega as novas partições nas tabelas do Athena """

    athena.start_query_execution(
        QueryString=f'MSCK REPAIR TABLE {database_name}.{table_name}',
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': f's3://{bucket}/report/athena-query-results/'})


def remove_old_files_from_stage(bucket, path_stage):
    """ Remove os arquivos antigos já processados da area de stage """

    lst_files = s3_fs.glob(f's3://{bucket}/{path_stage}*')

    for file in lst_files:
        if current_date not in file:
            s3.delete_object(Bucket=bucket, Key=file.replace(bucket + '/', ''))


def lambda_handler(event, context):
    event_bucket = event['Records'][0]['s3']['bucket']['name']
    event_key = event['Records'][0]['s3']['object']['key']
    event_time = event['Records'][0]['eventTime']
    event_region = event['Records'][0]['awsRegion']

    if 'input.csv' in event_key or 'output.csv' in event_key:
        data = get_data_s3(bucket=event_bucket,
                           key=event_key,
                           date=event_time,
                           region=event_region)

        join_stage_files(bucket=data.bucket,
                         path_event=data.path_event,
                         file_event=data.file_event,
                         path_stage=data.path_stage,
                         file_stage=data.file_stage,
                         folder_id_event=data.id_folder_event,
                         data_event=data.date_event)

        load_target_from_stage(bucket=data.bucket,
                               path_stage=data.path_stage,
                               table_name=data.table_target)

        load_athena_partitions(bucket=data.bucket,
                               table_name=data.table_target)

        remove_old_files_from_stage(bucket=data.bucket,
                                    path_stage=data.path_stage)
