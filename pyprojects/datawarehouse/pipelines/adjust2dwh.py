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

    send_telegram_message(1, f"""Pipeline {id_pipeline} has started""")

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
    last_modified = 0
    cnt = 0
    df = pd.DataFrame()
    for p in prefix:
        for obj in my_bucket.objects.filter(Prefix=p):
            if obj.key.endswith('csv.gz') and calendar.timegm(obj.last_modified.timetuple()) > delta:
                cnt += 1

                if calendar.timegm(obj.last_modified.timetuple()) >= last_modified:
                    last_modified = calendar.timegm(obj.last_modified.timetuple())

                df_obj = pd.read_csv(obj.get()['Body'], compression='gzip')
                df_obj["file_name"] = obj.key

                if len(df_obj.columns) > 1:
                    print(obj.key)
                    print(calendar.timegm(obj.last_modified.timetuple()))
                    if df.empty:
                        df = clean_pandas_dataframe(df_obj)
                    else:
                        df = concatenate_dataframes(df, clean_pandas_dataframe(df_obj))

    if cnt > 0:
        # create the offset delta info tp be written into datawarehouse
        delta_update = {"id_pipeline": f"""{id_pipeline}""", "delta": last_modified}
        delta_update = clean_pandas_dataframe(pd.DataFrame.from_dict([delta_update]), pipeline)
        try:
            write_to_gbq(args.conn,
                         args.schema,
                         'adjust_events',
                         clean_pandas_dataframe(df, pipeline, False, last_modified),
                         args.wtype, 'csv')
            write_to_gbq(args.conn,
                         'etl_metadata', 'airflow_run', delta_update, 'append')
        except Exception as e:
            print(f'caught {type(e)}: {str(e)}')

    send_telegram_message(1, f"""Pipeline {pipeline} has finished""")


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
