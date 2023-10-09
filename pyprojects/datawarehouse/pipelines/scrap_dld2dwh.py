"""
Script to bring pgdata from scrapping sales to Analytics datalake
"""

import argparse
from pyprojects.utils.udfs import *
import requests
import urllib.request
import json
import pandas as pd
from io import StringIO


def run(args):

    authority = get_creds(args.schema, args.btype, 'authority')

    headers = {
            'Accept': 'application/json, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'AppUser': '',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json; charset=UTF-8',
            'Origin': f'''https://{authority}''',
            'Referer': f'''https://{authority}/''',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            'consumer-id': 'gkb3WvEG0rY9eilwXC0P2pTz8UzvLj9F',
            'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

    json_data = {
        'P_FROM_DATE': args.date_from,
        'P_TO_DATE': args.date_to,
        'P_GROUP_ID': '',
        'P_IS_OFFPLAN': '',
        'P_IS_FREE_HOLD': '',
        'P_AREA_ID': '',
        'P_USAGE_ID': '',
        'P_PROP_TYPE_ID': '',
        'P_TAKE': '50000',
        'P_SKIP': '0',
        'P_SORT': 'TRANSACTION_NUMBER_ASC',
    }

    r = requests.post(f'''https://gateway.{authority}/open-data/transactions''', headers=headers, json=json_data)
    r = json.loads(r.content)
    df = pd.DataFrame()
    for el in r["response"]["result"]:
        df = pd.concat([df, pd.DataFrame.from_dict([el])])
    df = clean_pandas_dataframe(df, '', standartise=True)
    df = df.loc[:, ~df.columns.str.endswith('_ar')]
    write_to_gbq(args.conn, args.schema, dataset=args.btype,
                 dataframe=clean_pandas_dataframe(df, 'googlesheet2dwh'), wtype='append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with scrapping websites')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-business_type', dest='btype', required=True,
                        help="b2c or b2b")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    parser.add_argument('-date_from', dest='date_from', required=True,
                        help="from date mm/dd/yyyy")
    parser.add_argument('-date_to', dest='date_to', required=True,
                        help="to date mm/dd/yyyy")

    run(parser.parse_args())
