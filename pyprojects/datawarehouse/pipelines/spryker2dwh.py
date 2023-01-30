"""
Script to bring pgdata from Spryker to Analytics datalake
"""

import argparse
from pyprojects.utils.udfs import *
import boto3
import pandas as pd


def run(args):

    #s3 credentials
    s3 = boto3.resource(
    service_name='s3',
    region_name=get_creds(args.schema, 'spryker2dwh', 'region_name'),
    aws_access_key_id=get_creds(args.schema, 'spryker2dwh', 'aws_access_key_id'),
    aws_secret_access_key=get_creds(args.schema, 'spryker2dwh', 'aws_secret_access_key')
    )

    #navigate to the s3 bucket
    for bucket in s3.buckets.all():
        if bucket.name == 'stg-analytics-stream':
            print(bucket.name)
            my_bucket = s3.Bucket(bucket.name)
            df = pd.DataFrame()
            # roam through the files in the navigated s3 bucket
            for my_bucket_object in my_bucket.objects.all():
                print(my_bucket_object.key)
                print(f'START - reading from s3 - {datetime.datetime.now()}')
                data = my_bucket_object.get()['Body'].read().decode('utf-8')
                print(f'READING JSON START - {datetime.datetime.now()}')
                json_data = json.loads(data)
                print(json_data)
                print(f'READING JSON END - {datetime.datetime.now()}')
                for substuff in json_data:
                    if substuff in args.dataset:
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

            print(f'WRITE TO BQ START - {datetime.datetime.now()}')
            try:
                write_to_gbq(args.conn,
                             args.schema,
                             args.dataset,
                             clean_pandas_dataframe(df, 'spryker2dwh'),
                             args.wtype)
                print(f'WRITE TO BQ END - {datetime.datetime.now()}')
            except Exception as e:
                print(f'caught {type(e)}: {str(e)}')
                sys.exit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with spryker')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    parser.add_argument('-dataset_name', dest='dataset', required=True,
                        help="list of datasets to write in")
    parser.add_argument('-writing_type', dest='wtype', required=True,
                        help="append or replace")
    parser.add_argument('-date', dest='dt', required=False,
                        help="start date to get the data")
    run(parser.parse_args())

