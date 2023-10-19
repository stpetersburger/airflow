"""
Script to bring pgdata from scrapping leads to Analytics datalake
"""

import argparse
from pyprojects.utils.udfs import *
import requests
import urllib.request
import json
import pandas as pd
from io import StringIO


def run(args):

    pipeline = 'scrap_bv2dwh'
    send_telegram_message(1, f"""Pipeline {pipeline} has started""")

    if args.btype == 'bv':
        authority = get_creds(args.schema, args.btype, 'authority')
        url = get_creds(args.schema, args.btype, 'url').format(page_num=1)

        headers = {
            'authority': authority,
            'accept': 'application/vnd.api+json',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/vnd.api+json',
            'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
                          '(KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }

        r = requests.get(url, headers=headers)

        if r.status_code == 200:

            r = json.loads(r.content)
            metadata = r["data"]["relationships"]["properties"]["meta"]
            print(metadata)
            df = pd.DataFrame()
            for i in range(metadata["page_count"]):
                url = get_creds(args.schema, args.btype, 'url').format(page_num=i+1)
                r = requests.get(url, headers=headers)
                print(r.status_code)
                if r.status_code == 200:
                    r = r.json()
                    for c in r:
                        if c == 'included':
                            for el in r["included"]:
                                if el["type"] == 'property':
                                    df_stg = pd.DataFrame.from_dict([el["attributes"]])
                                    df_stg["listing_nk"] = el["id"]
                                    if "ask" in el["meta"]["price_text"].lower():
                                        df_stg["price_text"] = 0
                                    else:
                                        df_stg["price_text"] = 1

                                    df = pd.concat([df, df_stg])
                            print(i)
            # removing duplicates, keeping cleaned data in the same dataframe
            df.drop_duplicates("listing_nk")
            #checking for new columns
            check_pddf_structures(args.conn, args.schema, args.btype, df.columns.tolist())

            try:
                write_to_gbq(args.conn, args.schema, dataset=args.btype,
                             dataframe=clean_pandas_dataframe(df, 'googlesheet2dwh'), wtype='replace')
            except Exception as e:
                send_telegram_message(0, f' {pipeline} caught {type(e)}: {str(e)}')
                print(f'caught {type(e)}: {str(e)}')

            send_telegram_message(1, f"""Pipeline {pipeline} has finished""")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with scrapping websites')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-business_type', dest='btype', required=True,
                        help="b2c or b2b")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")

    run(parser.parse_args())

