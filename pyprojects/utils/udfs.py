"""
User defined functions

"""
import os
import sys
import datetime
import time
import json
import base64
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq as pd_gbq
import gspread as gs
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import io
import re


def get_creds(conn, pipeline, creds_name):

    with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/creds.json""") as f:
        creds = json.load(f)
        f.close()

    if creds[conn][pipeline][creds_name].isupper():
        return os.environ[creds[conn][pipeline][creds_name]]
    else:
        return creds[conn][pipeline][creds_name]


def get_etl_datatypes(etl, dataset=''):

    with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_schemas.json""") as f:
        etl_schemas = json.load(f)
        f.close()

        if dataset == '':
            return etl_schemas[etl]
        else:
            return etl_schemas[etl][dataset]
        

def get_data_from_googlesheet(conn, gsheet, gsheet_tab):
    
    gcp_credentials = gs.service_account_from_dict(json.loads(get_creds('gcp_bq', 'datawarehouse', 'google_cloud_platform')))
    gc_spreadsheet = gcp_credentials.open_by_url(f"""{get_creds('gs', 'spreadsheets', 'prefix')}{gsheet}""")

    gs_sheet = gc_spreadsheet.worksheet(gsheet_tab)

    return pd.DataFrame(gs_sheet.get_all_records())


def get_data_from_sharepoint(conn="ms_sharepoint", sheet='', sheet_tab='Sheet1'):

    username = get_creds(conn, 'spreadsheets', 'username')
    password = get_creds(conn, 'spreadsheets', 'password')

    sheet_url = f"""{get_creds(conn, 'spreadsheets', 'prefix')}{sheet}{'?web=1'}"""
    print(sheet_url)
    ctx_auth = AuthenticationContext(sheet_url)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(sheet_url, ctx_auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        print("Sharepoint authentication successful")
    response = File.open_binary(ctx, sheet_url)
    # save data to BytesIO stream
    bytes_file_obj = io.BytesIO()
    bytes_file_obj.write(response.content)
    bytes_file_obj.seek(0)  # set file object to start
    print("PASSED HERE")
    # read excel file and each sheet into pandas dataframe
    df = pd.read_excel(bytes_file_obj, sheet_name=sheet_tab, engine='openpyxl')

    return df


def get_data_from_url(url, file, file_type):

    full_url = f"""{url}/{file}"""
    print(full_url)
    if file_type == 'csv':
        return pd.read_csv(full_url)


def write_to_gbq(conn, schema, dataset, dataframe, wtype):
    # bigQuery credentials
    gcp_credentials = json.loads(get_creds(conn, 'datawarehouse', 'google_cloud_platform'))
    gcp_gbq_credentials = service_account.Credentials.from_service_account_info(gcp_credentials)

    #pandas_gbq definition
    pd_gbq.context.credentials = gcp_gbq_credentials
    pd_gbq.context.project = gcp_credentials['project_id']

    pd_gbq.to_gbq(dataframe, f'{schema}.{dataset}', if_exists=wtype)
    print(f'WRITE TO BQ END - {datetime.datetime.now()}')

def get_from_gbq(conn, str_sql):
    # bigQuery credentials
    gcp_credentials = json.loads(get_creds(conn, 'datawarehouse', 'google_cloud_platform'))
    gcp_gbq_credentials = service_account.Credentials.from_service_account_info(gcp_credentials)

    #pandas_gbq definition
    pd_gbq.context.credentials = gcp_gbq_credentials
    pd_gbq.context.project = gcp_credentials['project_id']

    return pd_gbq.read_gbq(str_sql,
                           progress_bar_type=None)


def clean_pandas_dataframe(df, pipeline=''):

    # get the initial names of the fields
    header_list = df.columns.tolist()
    # removes any non-numeric OR non-letter symbol in a column name into _ and lowers the register
    header_list_new = list(
        map(lambda i: re.sub('[^a-zA-Z0-9] *', '_',
                             re.sub(r'\B[A-Z]\B', lambda x: '_' + x.group().lower(),
                                    header_list[i])
                             ).lower(),
            range(0, len(header_list))))
    df.columns = header_list_new

    for col in header_list_new:
        if "_at" in col:
            df[col] = pd.to_datetime(df[col], utc=True)

    if pipeline != '':

        if pipeline in ['googlesheet2dwh', 'sharepoint2dwh', 'url']:

            df = df.astype(str)
            df = df[df.iloc[:, 0] != 'nan']

        elif pipeline in ['spryker2dwh']:

            datasets_schemas = get_etl_datatypes(pipeline)

            #changing the field format to be accepted by BQ
            for fld in datasets_schemas:
                if fld in header_list_new:
                    df[fld] = df[fld].astype("string")

        ins_dt = pd.to_datetime(datetime.datetime.utcnow(), utc=True)
        df["inserted_at"] = ins_dt

    return df


def get_s3_prefix(project='spryker', business_type='b2c', dt=''):

    prefix = []

    if dt == '' or datetime.datetime.strptime(dt, "%Y%m%d").date() > datetime.datetime.now().date():
        d = datetime.datetime.now().date() - datetime.timedelta(1)
    else:
        d = datetime.datetime.strptime(dt, "%Y%m%d").date()

    while not d > datetime.datetime.now().date():

        prefix.append(
            "{}/{}/{}/{}/{}/".format(d.year,
                                     d.month,
                                     d.day,
                                     project,
                                     business_type)
        )

        d = d+datetime.timedelta(1)

    return prefix


def get_delta(conn, pipeline_nk, dt=''):

    strsql = f"""select max(delta)  delta
               from etl_metadata.airflow_run 
              where pipeline_nk ='{pipeline_nk}'"""
    delta = get_from_gbq(conn, strsql)

    if pd.isnull(delta['delta'].iloc[0]):
        if dt == '':
            delta = 1675140678.0
        else:
            delta = time.mktime(datetime.datetime.strptime(dt, "%Y%m%d").timetuple())
    else:
        delta = delta['delta'].iloc[0]

    return delta


def get_deduplication_data(conn, entity, df_new, index):

    flds = ','.join(str(x) for x in index)

    if entity == 'spryker2dwh_b2c_items':
        strsql = f"""select  {flds},1 state
                       from  aws_s3.sales_order_item_states
                      where  updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 31 DAY)"""
    elif entity == 'spryker2dwh_b2c_orders':
        strsql = f"""select  {flds},1 state
                       from  aws_s3.sales_order_states
                              where  updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 31 DAY)"""

    df_old = get_from_gbq(conn, strsql)

    df_new = clean_pandas_dataframe(df_new, 'spryker2dwh')

    df = pd.merge(df_new, df_old, left_on=index,
                   right_on=index,
                   how='left',
                   suffixes=['', '_old'])

    return df.loc[df['state'].isnull()].drop('state', axis=1)


def concatenate_dataframes(df1, df2):
    if df1.empty:
        df1 = df2
    else:
        df1 = pd.concat([df1, df2])

    return df1
