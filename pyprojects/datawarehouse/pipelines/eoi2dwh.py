"""
Script to bring pgdata from Salesforce to Analytics datalake
"""

import argparse, math

import numpy as np

from pyprojects.utils.udfs import *
from simple_salesforce import Salesforce
import requests
import pandas as pd
from io import StringIO
from collections import OrderedDict
from collections import OrderedDict
import psycopg2


def run(args):
    connection = psycopg2.connect(database=get_creds('postgres', 'eoi', 'database'),
                                  user=get_creds('postgres', 'eoi', 'username'),
                                  password=get_creds('postgres', 'eoi', 'password'),
                                  host=get_creds('postgres', 'eoi', 'host'),
                                  port=get_creds('postgres', 'eoi', 'port'))

    cursor = connection.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")

    # Fetch all rows from database
    record = cursor.fetchall()
    for el in record:
        tbl = ''.join(el)
        print(tbl)
        tbl_fields = connection.cursor()
        tbl_data = connection.cursor()
        tbl_data.execute(f"""SELECT * FROM public.{tbl};""")
        tbl_fields.execute(f"""SELECT  column_name 
                                 FROM  information_schema.columns
                                WHERE  table_schema = 'public' AND table_name = '{tbl}'""")
        data = tbl_data.fetchall()

        columns = tbl_fields.fetchall()

        df = pd.DataFrame(data, columns=list(map(''.join, columns)))
        df = df.astype(str)
        write_to_gbq(args.conn, args.schema, f"""{tbl}""",
                     clean_pandas_dataframe(df, 'eoi', '', datetime.datetime.now()), 'replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with eoi data')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    run(parser.parse_args())
