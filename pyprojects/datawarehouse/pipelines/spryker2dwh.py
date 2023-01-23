"""
Script to bring data from Spryker to Analytics datalake
"""

import argparse
import datetime
import boto3
import pandas as pd
import pandas_gbq
import json
from pyprojects.utils.udfs import *
from google.cloud import bigquery
from google.oauth2 import service_account

projdir='/usr/local/airflow'

def run(args):

    #s3 credentials
    s3 = boto3.resource(
        service_name=get_creds('aws_s3', 'service_name'),
        region_name=get_creds('aws_s3', 'region_name'),
        aws_access_key_id=get_creds('aws_s3', 'aws_access_key_id'),
        aws_secret_access_key=get_creds('aws_s3', 'aws_secret_access_key')
    )

    #bigQuery credentials
    credentials = service_account.Credentials.from_service_account_file(
        f'{projdir}/pyprojects/creds/gcp_bq.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    #bigQuery service definition
    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    project = client.project

    #pandas_gbq definition
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project

    filepath = f'{projdir}/data'

    datasets_schemas = get_etl_datatypes('spryker2dwh')
    pipelines = args.dst.split()
    
    #navigate to the s3 bucket
    for bucket in s3.buckets.all():
        if bucket.name == 'stg-analytics-stream':
            print(bucket.name)
            my_bucket = s3.Bucket(bucket.name)
            df = pd.DataFrame()
            #roam through the files in the navigated s3 bucket
            for my_bucket_object in my_bucket.objects.all():
                print(my_bucket_object.key)
                print(f'START - reading from s3 - {datetime.datetime.now()}')
                data = my_bucket_object.get()['Body'].read().decode('utf-8')
                print(f'READING JSON START - {datetime.datetime.now()}')
                json_data = json.loads(data)
                print(json_data)
                print(f'READING JSON END - {datetime.datetime.now()}')
                for substuff in json_data:
                    #if substuff in ['order', 'items', 'customer']:
                    if substuff in pipelines:
                        print(f'READING DATE OUT OF DATAFRAME START - {datetime.datetime.now()}')
                        if isinstance(json_data[substuff], list):
                            for i in json_data[substuff]:
                                dfi = pd.DataFrame.from_dict([i])
                                if df.empty:
                                    df = dfi
                                else:
                                    df = pd.concat([df, dfi])
                        else:
                            dfi = pd.DataFrame.from_dict([json_data[substuff]])
                            if df.empty:
                                df = dfi
                            else:
                                df = pd.concat([df, dfi])

                        print(f'READING DATE OUT OF DATAFRAME END - {datetime.datetime.now()}')

            print(f'DF SCHEMA START - {datetime.datetime.now()}')
            for fld in datasets_schemas[pipelines[0]]:
                print(fld)
                df[fld] = df[fld].astype("string")
            print(f'DF SCHEMA END - {datetime.datetime.now()}')
            print(df)
            print(f'DF ARABIC CHANGE START - {datetime.datetime.now()}')
            df = df.replace(regex=r'[^\x00-\x7F]+',
                            value='')
            print(f'DF ARABIC CHANGE END - {datetime.datetime.now()}')

            print(f'WRITE TO BG START - {datetime.datetime.now()}')
            pandas_gbq.to_gbq(df, f'aws_s3.{pipelines[0]}',
                              project_id=project,
                              if_exists='replace')
            print(f'WRITE TO BG END - {datetime.datetime.now()}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with spryker')
    parser.add_argument('-environment', dest='env', required=False,
                        help="data environment as string of dev, tst, prod")
    parser.add_argument('-dataset', dest='dst', required=True,
                        help="list of datasets to write in")
    run(parser.parse_args())

