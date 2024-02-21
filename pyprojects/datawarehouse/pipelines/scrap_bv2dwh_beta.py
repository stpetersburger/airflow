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

    pipeline = 'scrap_bv2dwh_beta'
    send_telegram_message(1, f"""Pipeline {pipeline} has started""")

    if args.proptype == 'townhouse':
        idproptype = 22
    elif args.proptype == 'villa':
        idproptype = 35
    else:
        idproptype = 1 # apartment

    if args.btype == 'bv_beta':
        authority = get_creds(args.schema, args.btype, 'authority')
        url = get_creds(args.schema, args.btype, 'url').format(id_property_type=idproptype, page_num=1)

        headers = {
            'authority': 'www.propertyfinder.ae',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'referer': f'https://www.propertyfinder.ae/en/search?c=1&t={idproptype}&fu=0&ob=mr',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'x-nextjs-data': '1',
        }

        r = requests.get(
            f'https://www.propertyfinder.ae/en/search?c=1&t={idproptype}&fu=0&ob=mr&page=1',
            headers=headers,
        )

        if r.status_code == 200:
            print(type(r.content))
            sys.exit()
            r = json.loads(r.content)
            sys.exit()
            metadata = r["data"]["relationships"]["properties"]["meta"]
            print(metadata)
            df = pd.DataFrame()
            for i in range(metadata["page_count"]):
                url = get_creds(args.schema, args.btype, 'url').format(id_property_type=idproptype, page_num=i+1)
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
                                    print(df)
                            print(i)
            # removing duplicates, keeping cleaned data in the same dataframe
            df.drop_duplicates("listing_nk")
            #checking for new columns
            check_pddf_structures(args.conn, args.schema, args.btype, df.columns.tolist())

            try:
                write_to_gbq(args.conn, args.schema, dataset=args.btype,
                             dataframe=clean_pandas_dataframe(df, 'googlesheet2dwh'), wtype='append')
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
    parser.add_argument('-property_type', dest='proptype', required=True,
                        help="apartment, villa or townhouse")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")

    run(parser.parse_args())

