import argparse
from pyprojects.utils.udfs import *


def run(args):

    etl_config_spreadsheet = get_data_from_googlesheet(args.conn, get_creds('gs', 'spreadsheets', 'config_sheet'), 'etl_pipelines')
    etl_config_spreadsheet = etl_config_spreadsheet[etl_config_spreadsheet.iloc[:, 0] != 0]
    print(etl_config_spreadsheet)

    for index, row in etl_config_spreadsheet.iterrows():
        if row['pipeline'] == 'googlesheet2dwh':
            df = get_data_from_googlesheet(conn=args.conn,
                                           gsheet=row['url'],
                                           gsheet_tab=row['tab'])
            write_to_gbq(args.conn, 'gcp_gs', row['name'], clean_pandas_dataframe(df, row['pipeline']), 'replace')
        elif row['pipeline'] == 'sharepoint2dwh':
            df = get_data_from_sharepoint(sheet=row["url"],
                                          sheet_tab=row["tab"])
            write_to_gbq(args.conn, 'shpoint', row['name'], clean_pandas_dataframe(df, row['pipeline']), 'replace')
        elif row['pipeline'] == 'url':
            df = get_data_from_url(url=row['url'],
                                   file=row['tab'],
                                   file_type=row['tab'].split(".")[1])
            write_to_gbq(args.conn, row['dwh_schema'], row['name'], clean_pandas_dataframe(df, row['pipeline']), 'replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with spryker')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    run(parser.parse_args())
