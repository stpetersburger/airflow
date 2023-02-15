import argparse
from pyprojects.utils.udfs import *


def run(args):

    etl_config_spreadsheet = get_data_from_googlesheet(args.conn, get_creds('gs',
                                                                            'spreadsheets',
                                                                            'config_sheet'),
                                                                            'etl_pipelines')

    etl_config_spreadsheet = etl_config_spreadsheet[etl_config_spreadsheet.iloc[:, 0] != 0]

    if args.sheet is not None:
        etl_config_spreadsheet = etl_config_spreadsheet[etl_config_spreadsheet["name"] == args.sheet]


    for index, row in etl_config_spreadsheet.iterrows():

        wtype = 'replace'

        if row['truncate'] and row['dwh_schema'] != '':
            wtype = 'append'
            get_from_gbq('gcp_bq', f"""TRUNCATE TABLE {row['dwh_schema']}.{row['name']}""")

        if row['pipeline'] == 'googlesheet2dwh':
            df = get_data_from_googlesheet(conn=args.conn,
                                           gsheet=row['url'],
                                           gsheet_tab=row['tab'])
            df = clean_pandas_dataframe(df)
            df = df.drop_duplicates()

            write_to_gbq(args.conn, row['dwh_schema'], row['name'], clean_pandas_dataframe(df, row['pipeline']), wtype)

        elif row['pipeline'] == 'sharepoint2dwh':
            df = get_data_from_sharepoint(sheet=row["url"],
                                          sheet_tab=row["tab"])
            df = clean_pandas_dataframe(df)
            df = df.drop_duplicates()
            write_to_gbq(args.conn, row['dwh_schema'], row['name'], clean_pandas_dataframe(df, row['pipeline']), wtype)

        elif row['pipeline'] == 'url':
            df = get_data_from_url(url=row['url'],
                                   file=row['tab'],
                                   file_type=row['tab'].split(".")[1])
            if row['name'] == 'catalog_products':
                df = clean_pandas_dataframe(df).filter(items=['sku_id',
                                                              'concrete_sku',
                                                              'category_id',
                                                              'category_name',
                                                              'brand_name',
                                                              'product_name',
                                                              'concrete_product_name',
                                                              'available_quantity',
                                                              'concrete_product_active',
                                                              'concrete_price',
                                                              'gross_default_price',
                                                              'gross_original_price',
                                                              'net_default_price',
                                                              'net_original_price',
                                                              'merchant_name'])
                df.columns = ['id_sku_config',
                              'id_sku_simple',
                              'id_category',
                              'category_name_en',
                              'brand_name_en',
                              'config_name_en',
                              'simple_name_en',
                              'simple_quantity',
                              'if_simple_active',
                              'simple_price',
                              'gross_default_price',
                              'gross_original_price',
                              'net_default_price',
                              'net_original_price',
                              'merchant_name_en']
            df = df.drop_duplicates()
            write_to_gbq(args.conn, row['dwh_schema'], row['name'], clean_pandas_dataframe(df, row['pipeline']), wtype)

        elif row['pipeline'] == 'dimension':
            with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_queries/{row['tab']}.py""") as f:
                sqlstr = f.read()
            df = get_from_gbq('gcp_bq', sqlstr)
            df = df.sort_values(df.columns[0])
            df = df.drop_duplicates()
            write_to_gbq(args.conn, row['dwh_schema'], row['name'], clean_pandas_dataframe(df, row['pipeline']), 'replace')

        elif row['pipeline'] == 'fact':
            with open(f"""{os.environ["AIRFLOW_HOME"]}/pyprojects/datawarehouse/etl_queries/{row['tab']}.py""") as f:
                sqlstr = f.read()
            #delete incremental part
            if row['incr_field'] != '':

                del_sql = f"""DELETE FROM {row['dwh_schema']}.{row['name']} WHERE {row['incr_field']} \
                                                                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)"""
                get_from_gbq('gcp_bq', del_sql)

                df = get_from_gbq('gcp_bq', sqlstr)
                df = df.sort_values(df.columns[0])
                df = df.drop_duplicates()
                write_to_gbq(args.conn, row['dwh_schema'], row['name'], clean_pandas_dataframe(df, row['pipeline']), 'append')
            else:
                get_from_gbq('gcp_bq', sqlstr)

        if row['if_historical']:
            #weekly snapshot
            strsql = f"""SELECT (CURRENT_DATE() - 7) = MAX(DATE(inserted_at))
                         FROM {row['dwh_schema']}.historical_{row['name']}
                         WHERE DATE(inserted_at) > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)"""
            if get_from_gbq('gcp_bq', strsql)["f0_"].iloc[0]:
                wtype = 'append'
                hist_fields = row['historical_fields'].split(',')
                df = df.filter(items=hist_fields)
                df = df.drop_duplicates()
                write_to_gbq(args.conn, row['dwh_schema'], f"""historical_{row['name']}""",
                             clean_pandas_dataframe(df, row['pipeline']), wtype)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with spryker')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-sheet', dest='sheet', required=False,
                        help="name of the sheet from etl_config")
    run(parser.parse_args())
