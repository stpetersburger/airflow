"""
User defined functions

"""
import os
import sys
import datetime
import json
import base64
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq


def get_creds(conn_name, conn_section=''):

    with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/creds/{conn_name}.json""") as f:
        creds = json.load(f)
        f.close()

        if conn_section == '':
            return creds
        else:
            return creds[conn_section]


def get_etl_datatypes(etl, dataset=''):

    with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_schemas.json""") as f:
        etl_schemas = json.load(f)
        f.close()

        if dataset == '':
            return etl_schemas[etl]
        else:
            return etl_schemas[etl][dataset]


def write_to_gbq(conn, schema, dataset, dataframe, wtype):
    # bigQuery credentials

    qbq_project = get_creds(conn, 'project_id')
    gbq_credentials = service_account.Credentials.from_service_account_info(get_creds('gcp_bq'))

    gbq = bigquery.Client(credentials=gbq_credentials, )

    #pandas_gbq definition
    pandas_gbq.context.credentials = gbq_credentials
    pandas_gbq.context.project = qbq_project

    pandas_gbq.to_gbq(dataframe, f'{schema}.{dataset}', if_exists=wtype)
    print(f'WRITE TO BQ END - {datetime.datetime.now()}')
