"""
User defined functions

"""
import os
import sys
import datetime
import calendar
import boto3
import pandas as pd
import time
import json
import base64
from google.cloud import bigquery as bq
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import pandas_gbq as pd_gbq
import gspread as gs
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import io
import re
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/creds.json""") as f:
    creds = json.load(f)
    f.close()

with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_data_formats.json""") as f:
    etl_data_formats = json.load(f)
    f.close()

with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_schemas.json""") as f:
    etl_schemas = json.load(f)
    f.close()


def get_creds(conn, pipeline, creds_name):

    if creds[conn][pipeline][creds_name][0].isupper():
        return os.environ[creds[conn][pipeline][creds_name]]
    else:
        return creds[conn][pipeline][creds_name]


def get_etl_datatypes(etl, dataset=''):

    if dataset == '':
        return etl_data_formats[etl]
    else:
        return etl_data_formats[etl][dataset]


def get_etl_schema(pipeline, message_object, message_method):

    msg_config = etl_schemas[pipeline][message_object]
    l = []
    for k, v in msg_config.items():
        if message_method in k:
            if isinstance(v, dict):
                if k == 'rename':
                    return v
                else:
                    for kk, vv in v.items():
                        l = l + vv.split(",")
            else:
                l = l + v.split(",")
    return l


def get_data_from_googlesheet(conn, gsheet, gsheet_tab):
    
    gcp_credentials = gs.service_account_from_dict(json.loads(get_creds(conn, 'bq',
                                                                              'google_cloud_platform')))
    gc_spreadsheet = gcp_credentials.open_by_url(f"""{get_creds('gcp', 'gs', 'prefix')}{gsheet}""")

    gs_sheet = gc_spreadsheet.worksheet(gsheet_tab)

    return pd.DataFrame(gs_sheet.get_all_records())


def write_data_to_googlesheet(conn, gsheet_tab, df):

    gcp_credentials = gs.service_account_from_dict(json.loads(get_creds(conn, 'bq', 'google_cloud_platform')))
    gc_spreadsheet = gcp_credentials.open_by_url(f"""{get_creds('gcp', 'gs', 
                                                                'prefix')}{get_creds('gcp', 'gs', 'output_sheet')}""")
    df = df.astype(str)
    try:
        gs_sheet = gc_spreadsheet.worksheet(gsheet_tab)
        gs_sheet.clear()
        gs_sheet.update([df.columns.values.tolist()] + df.values.tolist())
    except Exception as e:
        print(f'caught {type(e)}: {str(e)}')
        send_telegram_message(0, f"""OUTPUT Googlesheet - {gsheet_tab} caught {type(e)}: {str(e)}""")


def get_data_from_sharepoint(conn="ms", sheet='', sheet_tab='Sheet1'):

    username = get_creds(conn, 'o365', 'username')
    password = get_creds(conn, 'o365', 'password')

    sheet_url = f"""{get_creds(conn, 'sharepoint', 'prefix')}{sheet}{'?web=1'}"""
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
    # read excel file and each sheet into pandas dataframe
    df = pd.read_excel(bytes_file_obj, sheet_name=sheet_tab, engine='openpyxl')

    return df


def get_data_from_url(url, file, file_type):

    full_url = f"""{url}/{file}"""
    print(full_url)
    if file_type == 'csv':
        return pd.read_csv(full_url)


def write_to_gbq(conn, schema, dataset, dataframe, wtype, method=''):
    # bigQuery credentials
    gcp_credentials = json.loads(get_creds(conn, 'bq', 'google_cloud_platform'))
    gcp_gbq_credentials = service_account.Credentials.from_service_account_info(gcp_credentials)

    #pandas_gbq definition
    pd_gbq.context.credentials = gcp_gbq_credentials
    pd_gbq.context.project = gcp_credentials['project_id']
    if not dataframe.empty:
        try:
            print(f'WRITE TO BQ START - {datetime.datetime.now()}')
            if method == 'csv':
                pd_gbq.to_gbq(dataframe, f'{schema}.{dataset}', if_exists=wtype, api_method='load_csv')
            elif method == '':
                pd_gbq.to_gbq(dataframe, f'{schema}.{dataset}', if_exists=wtype)
            print(f'WRITE TO BQ END - {datetime.datetime.now()}')
        except Exception as e:
            print(f'caught {type(e)}: {str(e)}')
            send_telegram_message(0, f"""BQ {schema}.{dataset}: ERROR caught {type(e)}: {str(e)}""")
    else:
        print(f"""{dataset} - is empty""")
        send_telegram_message(1, f"""BQ - {wtype} - {schema}.{dataset} is empty""")


def get_from_gbq(conn, str_sql, etl_desc='', note=''):
    # bigQuery credentials
    gcp_credentials = json.loads(get_creds(conn, 'bq', 'google_cloud_platform'))
    gcp_gbq_credentials = service_account.Credentials.from_service_account_info(gcp_credentials)

    # pandas_gbq definition
    pd_gbq.context.credentials = gcp_gbq_credentials
    pd_gbq.context.project = gcp_credentials['project_id']

    try:
        df = pd_gbq.read_gbq(str_sql, progress_bar_type=None)
        return clean_pandas_dataframe(df)
    except Exception as e:
        print(f'caught {type(e)}: {str(e)}')
        send_telegram_message(0, f"""execute BQ {etl_desc} - {note}: ERROR caught {type(e)}: {str(e)}""")


def execute_gbq(conn, str_sql, etl_desc='', note=''):
    # bigQuery credentials
    gcp_credentials = json.loads(get_creds(conn, 'bq', 'google_cloud_platform'))
    gcp_gbq_credentials = service_account.Credentials.from_service_account_info(gcp_credentials)
    client = bq.Client(credentials=gcp_gbq_credentials)

    # pandas_gbq definition
    pd_gbq.context.credentials = gcp_gbq_credentials
    pd_gbq.context.project = gcp_credentials['project_id']
    try:

        #pd_gbq.read_gbq(str_sql, progress_bar_type=None)
        query_job = client.query(str_sql)
        print(query_job.result())

    except Exception as e:
        print(f'caught {type(e)}: {str(e)}')
        send_telegram_message(0, f"""execute BQ {etl_desc} - {note}: ERROR caught {type(e)}: {str(e)}""")


def clean_pandas_dataframe(df, pipeline='', standartise=False, batch_num=''):

    if pipeline == '':

        # get the initial names of the fields
        header_list = df.columns.tolist()
        # removes any non-numeric OR non-letter symbol in a column name into _ and lowers the register
        if standartise:
            header_list_new = list(
                map(lambda i: re.sub('[^a-zA-Z0-9] *', '_',
                                     re.sub(r'\B([A-Z]{1})[a-z]{1,2}|(ID)', lambda x: '_' + x.group().lower(),
                                            header_list[i])
                                     ).lower(),
                                    range(0, len(header_list))
                    )
                                    )
        else:
            header_list_new = list(
                map(lambda i: re.sub('[^a-zA-Z0-9] *', '_',
                                     header_list[i]
                                     ).lower(),
                    range(0, len(header_list))))

        df.columns = header_list_new

        for col in df.columns.tolist():
            if "_at" in col:
                if str(df[col].dtypes) == 'int64':
                    df[col] = pd.to_datetime(df[col], unit='s', utc=True)
                else:
                    df[col] = pd.to_datetime(df[col], utc=True)

    else:
        if pipeline in ['googlesheet2dwh', 'sharepoint2dwh']:

            #make all data string
            df = df.replace('', np.nan).fillna(np.nan).astype(str)
            #filter out all lines with empty first field
            df = df[df.iloc[:, 0] != 'nan']

        elif pipeline in ['spryker2dwh', 'url', 'ga2dwh']:

            datasets_schemas = get_etl_datatypes(pipeline)

            #changing the field format to be accepted by BQ
            for fld in datasets_schemas:
                if fld in df.columns.tolist():
                    df[fld] = df[fld].fillna(np.nan).astype("string")

            for col in df.columns.tolist():
                if "price" in col or "amount" in col or "rate" in col or "quantity" in col:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(np.nan).astype('float')
                elif df[col].dtypes == 'str':
                    # remove non-ASCII symbols from dataframe
                    df[col] = df[col].str.encode('ascii', 'ignore').str.decode('ascii')

        if pipeline not in ['fact', 'dimension']:
            if pipeline in ['ga2dwh']:
                for col in df.columns.tolist():
                    if df[col].dtypes == 'object':
                        df[col] = df[col].astype("string")
                df["event_timestamp"] = pd.to_datetime(df["event_timestamp"]*1000, unit='ns')
                df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], format='%Y-%m-%d %H:%M:%S.%f')
                df["event_date"] = pd.to_numeric(df["event_date"]).astype('int')
                # add inserted_at as the first column. helps on first runs of an entity
                df.insert(0, "inserted_at", pd.to_datetime(datetime.datetime.utcnow(), utc=True))
            else:
                if batch_num != '':
                    df["inserted_at"] = batch_num
                else:
                    df["inserted_at"] = pd.to_datetime(datetime.datetime.utcnow(), utc=True)

    return df


def get_s3_prefix(project='spryker', business_type='b2c', dt=''):

    prefix = []

    if dt == '' or datetime.datetime.strptime(dt, "%Y%m%d").date() > datetime.datetime.now().date():
        d = datetime.datetime.now().date() - datetime.timedelta(3)
    else:
        d = datetime.datetime.strptime(dt, "%Y%m%d").date()

    while not d > datetime.datetime.now().date():
        if project == 'spryker':
            dp = "{}/{}/{}/{}/{}/".format(d.year,
                                          d.month,
                                          d.day,
                                          project,
                                          business_type)
            prefix.append(dp)
            dp2 = "{}/{}/{}/{}/{}/".format(d.year,
                                           d.strftime('%m'),
                                           d.strftime('%d'),
                                           project,
                                           business_type)
            prefix.append(dp2)
        elif project == 'adjust':
            dp = "ogo3m90eil8g_{}".format(d, "YYYY-MM-DD")
            prefix.append(dp)

        d = d+datetime.timedelta(1)

    return prefix


def get_delta(conn, id_pipeline, dt=''):

    if dt == '':
        strsql = f"""select max(delta)  delta
                       from etl_metadata.airflow_run 
                      where id_pipeline ='{id_pipeline}'"""

        delta = get_from_gbq(conn, strsql, id_pipeline, 'get_delta')

        if pd.isnull(delta['delta'].iloc[0]):
            delta = 0
        else:
            delta = delta['delta'].iloc[0]
    else:
        delta = calendar.timegm(datetime.datetime.strptime(dt, "%Y%m%d").timetuple())

    return delta


def get_deduplication_data(conn, btype, entity, df_new, index):

    flds = ','.join(str(x) for x in index)

    if entity == 'spryker2dwh_items':
        strsql = f"""select  {flds},1 state
                       from  aws_s3.{btype}_sales_order_item_states
                      where  created_at  >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 62 DAY)"""

    df_old = get_from_gbq(conn, strsql, entity, 'deduplication')
    df_new = clean_pandas_dataframe(df_new)

    df = pd.merge(df_new, df_old, left_on=index,
                   right_on=index,
                   how='left',
                   suffixes=['', '_old'])

    df = df.loc[df['state'].isnull()].drop('state', axis=1)
    df = df.sort_values('updated_at', ascending=False).drop_duplicates(index).sort_index()
    df = df.groupby(index).max()

    return df


def concatenate_dataframes(df1, df2):

    if df1.empty:
        df1 = df2
    else:
        df1 = pd.concat([df1, df2])

    return df1


def get_gbq_dim_data(conn, dataset, table, fields):

    if isinstance(fields, list):
        flds = ','.join(str(x) for x in fields)
    else:
        flds = fields

    strsql = f"""SELECT {flds} FROM {dataset}.{table}"""

    return get_from_gbq(conn, strsql, table, dataset)


def send_telegram_message(msg_type, msg):

        apiToken = get_creds('watchers', 'telegram', 'token')
        chatID = get_creds('watchers', 'telegram', 'chat_id')
        apiURL = f'https://api.telegram.org/bot{apiToken}/sendMessage'

        if msg_type == 1:
            message = f"""\u2705 {msg}"""
        else:
            message = f"""\U0001F6A8 {msg}"""

        try:
            requests.post(apiURL, json={'chat_id': chatID, 'text': message})
        except Exception as e:
            print(e)


def send_email(email_from, email_to, text):
    message = text
    s = smtplib.SMTP(host='smtp.office365.com', port=587)
    s.starttls()
    username = get_creds('ms', 'o365', 'username')
    password = get_creds('ms', 'o365', 'password')
    print(username)
    s.login(username, password)
    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = email_to
    msg['Subject'] = "Subject to be mentioned for the report sent via this email"
    msg.attach(MIMEText(message, 'plain'))
    s.send_message(msg)
    del msg
    s.quit()
