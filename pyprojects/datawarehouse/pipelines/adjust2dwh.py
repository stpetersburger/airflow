"""
Script to bring pgdata from Adjust to Analytics datalake
"""

import argparse
from pyprojects.utils.udfs import *
import boto3
import pandas as pd


def run(args):
    project = 'adjust'
    pipeline = f"""{project}2dwh"""
    id_pipeline = pipeline

    # s3 credentials
    s3 = boto3.resource(
        service_name='s3',
        region_name=get_creds(args.schema, pipeline, 'region_name'),
        aws_access_key_id=get_creds(args.schema, pipeline, 'aws_access_key_id'),
        aws_secret_access_key=get_creds(args.schema, pipeline, 'aws_secret_access_key')
    )

    delta = get_delta(args.conn, id_pipeline, args.dt)
    print(delta)

    my_bucket = s3.Bucket(get_creds(args.schema, pipeline, 'bucket'))
    prefix = get_s3_prefix(project=project,
                           dt=args.dt)
    print(prefix)
    cnt = 0

    for p in prefix:
        for obj in my_bucket.objects.filter(Prefix=p):
            if obj.key.endswith('csv.gz') and calendar.timegm(obj.last_modified.timetuple()) > delta:
                cnt += 1
                print(obj.key)
                print(calendar.timegm(obj.last_modified.timetuple()))

                response = s3.get_object(Bucket=AWS_S3_BUCKET, Key="files/books.csv")

                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

                if status == 200:
                    print(f"Successful S3 get_object response. Status - {status}")
                    books_df = pd.read_csv(response.get("Body"))
                    print(books_df)
                else:
                    print(f"Unsuccessful S3 get_object response. Status - {status}")

                df = pd.read_csv(obj.get()['Body'].read(),
                                 compression='gzip')
                print(df)
                sys.exit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with spryker')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    parser.add_argument('-writing_type', dest='wtype', required=True,
                        help="append or replace")
    parser.add_argument('-date', dest='dt', required=False,
                        help="start date to get the data")
    run(parser.parse_args())
