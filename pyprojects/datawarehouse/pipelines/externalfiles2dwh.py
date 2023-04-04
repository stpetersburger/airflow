import argparse
from pyprojects.utils.udfs import *


def run(args):
    pipeline = 'externalfiles2dwh'
    send_telegram_message(1, f"""Pipeline {pipeline} has started""")

    etl_config_spreadsheet = get_data_from_googlesheet(args.conn, get_creds('gcp',
                                                                            'gs',
                                                                            'config_sheet'),
                                                                            'etl_pipelines')

    etl_config_spreadsheet = etl_config_spreadsheet[etl_config_spreadsheet.iloc[:, 0] != 0]

    if args.schedule is not None:
        etl_config_spreadsheet = etl_config_spreadsheet[etl_config_spreadsheet["schedule_type"] == args.schedule]

    for index, row in etl_config_spreadsheet.iterrows():
        print(row['tab'])
        df = pd.DataFrame()
        wtype = 'replace'

        if row['truncate'] and row['dwh_schema'] != '':
            wtype = 'append'
            for s in row["dwh_schema"].split('|'):
                execute_gbq('gcp', f"""TRUNCATE TABLE {s}.{row['name']}""", row['name'], 'truncate')
        if row['pipeline'] == 's3':
            etl_s3 = boto3.resource(
                service_name='s3',
                region_name=get_creds(row['dwh_schema'], row['url'], 'region_name'),
                aws_access_key_id=get_creds(row['dwh_schema'], row['url'], 'aws_access_key_id'),
                aws_secret_access_key=get_creds(row['dwh_schema'], row['url'], 'aws_secret_access_key')
            )
            etl_bucket = etl_s3.Bucket(row['tab'])

            for p in row['name'].split('|'):
                for obj in etl_bucket.objects.filter(Prefix=p):
                    print(obj.key)
                    file_data = obj.get()['Body'].read().decode('utf-8')
                    obj_info = obj.key.split('.')
                    if obj_info[1] == 'csv':
                        df = pd.read_csv(io.StringIO(file_data))
                        write_to_gbq(args.conn,
                                     row['dwh_schema'],
                                     f"""{row['business_type']}_{obj_info[0]}""",
                                     clean_pandas_dataframe(df, '', True),
                                     wtype)
                        df = pd.DataFrame()

        elif row['pipeline'] == 'googlesheet2dwh':
            df = get_data_from_googlesheet(conn=args.conn,
                                           gsheet=row['url'],
                                           gsheet_tab=row['tab'])

        elif row['pipeline'] == 'sharepoint2dwh':
            df = get_data_from_sharepoint(sheet=row["url"],
                                          sheet_tab=row["tab"])

        elif row['pipeline'] == 'url':
            df = get_data_from_url(url=row['url'],
                                   file=row['tab'],
                                   file_type=row['tab'].split(".")[1])
            if row['name'] == 'b2c_catalog_products':
                df = clean_pandas_dataframe(df).filter(items=get_etl_schema(pipeline, row['name'], 'filter'))
                df.rename(columns=get_etl_schema(pipeline, row['name'], 'rename'), inplace=True)

        elif row['pipeline'] == 'dimension':
            with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_queries/{row['tab']}.py""") as f:
                sqlstr = f.read()
            i = 0
            for b in row['business_type'].split('|'):
                s = row["dwh_schema"].split("|")[i]
                df = get_from_gbq('gcp', sqlstr.format(b, s), row['pipeline'], row['name'])
                df = df.sort_values(df.columns[0])
                write_to_gbq(args.conn, s, row['name'], clean_pandas_dataframe(df, row['pipeline']), wtype)
                i += 1
                df = pd.DataFrame()

        elif row['pipeline'] == 'fact' or row['pipeline'] == 'query':
            with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_queries/{row['tab']}.py""") as f:
                sqlstr = f.read()
            i = 0
            for b in row['business_type'].split('|'):
                s = row["dwh_schema"].split('|')[i]
                ii = 0
                for t in row["multiple_tables"].split('|')[ii].split(','):
                    if row['incr_field'] != '':
                        wtype = 'append'
                        # delete incremental part
                        del_sql = f"""DELETE FROM {s}.{row["name"]} WHERE {row['incr_field']} \
                                                                            >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), \
                                                                            INTERVAl 3 HOUR)), INTERVAL {row['incr_interval']})"""
                        # if multiple_table run
                        if len(t) > 0:
                            print(t)
                            del_sql = del_sql + f" AND {row['url']} = '{t}'"
                            sql_str = sqlstr.format(b, s, incr_interval=row['incr_interval'],
                                                      # added on th 31.03.2023 for GA data
                                                      event_name=t
                                                    )
                        else:
                            sql_str = sqlstr.format(b, s, incr_interval=row['incr_interval'],
                                                    # as b2b doesn't have exchange rate to be used
                                                    exchange_rate=0 if b == "b2b" else "MIN(b.order_exchange_rate)",
                                                    # added on 29.03.2023 as b2b doesn't have loyalty points redemption
                                                    points_redeemed="MIN(b.points_redeemed)" if b == "b2c" else 0,
                                                    # added on 29.03.2023 as b2b doesn't have order channel (web/app)
                                                    channel="MIN(b.channel)" if b == "b2c" else "'no_channel'"
                                                    )
                        execute_gbq('gcp', del_sql, f"""{s}.{b}""", 'incremental deletion')
                        df = get_from_gbq('gcp', sql_str, row['pipeline'], row['name'],)
                        df = df.sort_values(df.columns[0])
                        if row['output'] != '':
                            o = row['output'].split('|')
                            if not i+1 > len(o):
                                dfo = df.filter(items=row['output_fields'].split('|'))
                                write_data_to_googlesheet(conn=args.conn, gsheet_tab=f"""{row['output']}_{b}""", df=dfo)
                    else:
                        df = get_from_gbq('gcp',
                                          sqlstr.format(b, s),
                                          f"""{s}.{row['name']}""",
                                          row['name'])
                    write_to_gbq(args.conn, s, row['name'], clean_pandas_dataframe(df, row['pipeline']), wtype)

                    # config iterations
                    i += 1

                    if len(row["multiple_tables"].split('|')) > 1:
                        ii = i
                    else:
                        ii = 0

                df = pd.DataFrame()

        if not df.empty:
            df = clean_pandas_dataframe(df).drop_duplicates()

            write_to_gbq(args.conn, row['dwh_schema'], row['name'], clean_pandas_dataframe(df, row['pipeline']), wtype)

        if row['if_historical']:
            # weekly snapshot
            strsql = f"""SELECT MOD(DATE_DIFF(CURRENT_DATE(), COALESCE(MAX(DATE(inserted_at)),'2023-03-08'), DAY),7) = 0
                         FROM {row['dwh_schema']}.historical_{row['name']}
                         WHERE DATE(inserted_at) > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)"""

            if get_from_gbq('gcp', strsql, row['name'], 'historical')["f0_"].iloc[0]:
                wtype = 'append'
                hist_fields = row['historical_fields'].split('|')
                df = df.filter(items=hist_fields)
                df = df.drop_duplicates()
                write_to_gbq(args.conn, row['dwh_schema'], f"""historical_{row['name']}""",
                             clean_pandas_dataframe(df, row['pipeline']), wtype)

    send_telegram_message(1, f"""Pipeline {pipeline} has finished""")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with external files')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schedule_type', dest='schedule', required=False,
                        help="schedule type from the config: hourly, daily, etc.")
    run(parser.parse_args())
