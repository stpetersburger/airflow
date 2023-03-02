import argparse
from pyprojects.utils.udfs import *


def run(args):
    pipeline = 'externalfiles2dwh'
    send_telegram_message(1, f"""Pipeline {pipeline} has started""")

    etl_config_spreadsheet = get_data_from_googlesheet(args.conn, get_creds('gs',
                                                                            'spreadsheets',
                                                                            'config_sheet'),
                                                                            'etl_pipelines')

    etl_config_spreadsheet = etl_config_spreadsheet[etl_config_spreadsheet.iloc[:, 0] != 0]

    if args.sheet is not None:
        etl_config_spreadsheet = etl_config_spreadsheet[etl_config_spreadsheet["name"] == args.sheet]

    for index, row in etl_config_spreadsheet.iterrows():

        print(row['tab'])
        df = pd.DataFrame()
        wtype = 'replace'

        if row['truncate'] and row['dwh_schema'] != '':
            wtype = 'append'
            get_from_gbq('gcp_bq', f"""TRUNCATE TABLE {row['dwh_schema']}.{row['name']}""")

        if row['pipeline'] == 'googlesheet2dwh':
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
            if row['name'] == 'catalog_products':
                df = clean_pandas_dataframe(df).filter(items=get_etl_schema(pipeline, row['name'], 'filter'))
                df.rename(columns=get_etl_schema(pipeline, row['name'], 'rename'), inplace=True)

        elif row['pipeline'] == 'dimension':
            with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_queries/{row['tab']}.py""") as f:
                sqlstr = f.read()
            df = get_from_gbq('gcp_bq', sqlstr)
            df = df.sort_values(df.columns[0])

        elif row['pipeline'] == 'fact':
            with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_queries/{row['tab']}.py""") as f:
                sqlstr = f.read()
            if row['incr_field'] != '':
                wtype = 'append'
                # delete incremental part
                del_sql = f"""DELETE FROM {row['dwh_schema']}.{row['name']} WHERE {row['incr_field']} \
                                                                    >= DATE_SUB(DATE(DATE_ADD(CURRENT_TIMESTAMP(), \
                                                                    INTERVAl 3 HOUR)), INTERVAL 1 MONTH)"""
                get_from_gbq('gcp_bq', del_sql)

                df = get_from_gbq('gcp_bq', sqlstr)
                df = df.sort_values(df.columns[0])
            else:
                get_from_gbq('gcp_bq', sqlstr)

        if not df.empty:
            df = clean_pandas_dataframe(df).drop_duplicates()

            write_to_gbq(args.conn, row['dwh_schema'], row['name'], clean_pandas_dataframe(df, row['pipeline']), wtype)

            if row['output'] != '':
                df = df.filter(items=row['output_fields'].split('|'))
                write_data_to_googlesheet(conn=args.conn, gsheet_tab=row['output'], df=df)

        if row['if_historical']:
            # weekly snapshot
            strsql = f"""SELECT (CURRENT_DATE() - 7) = MAX(DATE(inserted_at))
                         FROM {row['dwh_schema']}.historical_{row['name']}
                         WHERE DATE(inserted_at) > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)"""

            if get_from_gbq('gcp_bq', strsql)["f0_"].iloc[0]:
                wtype = 'append'
                hist_fields = row['historical_fields'].split('|')
                df = df.filter(items=hist_fields)

                df = df.drop_duplicates()
                write_to_gbq(args.conn, row['dwh_schema'], f"""historical_{row['name']}""",
                             clean_pandas_dataframe(df, row['pipeline']), wtype)

    send_telegram_message(1, f"""Pipeline {pipeline} has finished""")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with spryker')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-sheet', dest='sheet', required=False,
                        help="name of the sheet from etl_config")
    run(parser.parse_args())
