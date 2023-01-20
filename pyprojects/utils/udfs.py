"""
User defined functions

"""
import os
import json
import base64
from sqlalchemy import create_engine, exc
#import cx_Oracle
import pandas as pd
import sys
import urllib.parse


def get_creds(conn_name,conn_section=''):

    with open(f"""{os.environ["AIRFLOW_USER_HOME"]}/pyprojects/creds/{conn_name}.json""") as f:
        creds = json.load(f)
        f.close()

    if conn_section == '':
        return creds
    else:
        return creds[conn_section]

def get_etl_datatypes(etl,dataset=''):

    with open(f"""{os.environ["AIRFLOW_USER_HOME"]}/pyprojects/datawarehouse/etl_schemas.json""") as f:
        etl_schemas = json.load(f)
        f.close()

    if dataset == '':
        return etl_schemas[etl]
    else:
        return etl_schemas[etl][dataset]
