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

    pipeline = 'scrap_tp2dwh'
    send_telegram_message(1, f"""Pipeline {pipeline} has started""")

    if args.btype == 'tp':
        authority = get_creds(args.schema, args.btype, 'authority')
        url = get_creds(args.schema, args.btype, 'url')

        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Origin': authority,
            'Referer': authority,
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded',
            'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        data = data = get_creds(args.schema, args.btype, 'data').replace('page_num', '1')

        r = requests.post(
            url=url,
            headers=headers,
            data=data,
        )

        if r.status_code == 200:

            r = json.loads(r.content)
            print(r["results"][0]["nbHits"])
            df = pd.DataFrame()
            row_index = 0
            for i in range(r["results"][0]["nbPages"]):

                data = get_creds(args.schema, args.btype, 'data').replace('page_num', f'{i+1}')
                r = requests.post(
                    url=url,
                    headers=headers,
                    data=data,
                )
                print(r.status_code)
                if r.status_code == 200:

                    df_stg = pd.DataFrame(columns=get_etl_schema(pipeline=pipeline,
                                                                 message_object='tp',
                                                                 message_method='fields'))

                    r = r.json()
                    r = r["results"][0]["hits"]
                    for c in r:
                        df_stg.loc[row_index, "name"] = str(c["name"]["en"])
                        df_stg.loc[row_index, "size"] = float(c["size"])
                        df_stg.loc[row_index, "size_unit_identifier"] = "sqft"
                        df_stg.loc[row_index, "price_period_label"] = "AED"
                        df_stg.loc[row_index, "offering_type"] = str(c["categories"]["name"]["en"][1])
                        df_stg.loc[row_index, "verified"] = bool(c["is_verified"])
                        df_stg.loc[row_index, "furnished"] = c["furnished"]
                        df_stg.loc[row_index, "reference"] = c["property_reference"]
                        df_stg.loc[row_index, "default_price"] = c["price"]
                        df_stg.loc[row_index, "category_identifier"] = c["categories"]["name"]["en"][0]
                        df_stg.loc[row_index, "type_identifier"] = c["categories"]["slug"][0]
                        if c["agent"] is not None:
                            df_stg.loc[row_index, "broker_id"] = c["agent"]["name"]["en"]
                        if len(c["agent_profile"]) > 0:
                            df_stg.loc[row_index, "agent_id"] = c["agent_profile"]["id"]
                        else:
                            df_stg.loc[row_index, "agent_id"] = -9999

                        lll = ''
                        if c["is_premium_ad"]:
                            lll = lll + '|' + 'premium'
                        if c["highlighted_ad"]:
                            lll = lll + '|' + 'highlighted'
                        if c["featured_listing"]:
                            lll = lll + '|' + 'featured'
                        if c["promoted"]:
                            lll = lll + '|' + 'promoted'
                        df_stg.loc[row_index, "listing_level_label"] = lll

                        df_stg.loc[row_index, "bathroom_value"] = c["bathrooms"]
                        df_stg.loc[row_index, "bedroom_value"] = c["bedrooms"]

                        ln = c["neighborhoods"]["name"]["en"]

                        if ln is not None:
                            if len(ln) > 1:
                                ln = '|/|'.join(ln)
                            else:
                                if len(ln) != 0:
                                    ln = ln[0]
                                else:
                                    ln = 'unspecified_area'
                        else:
                            ln = 'unspecified_area'

                        if c["building"] is not None:
                            b = c["building"]["name"]["en"]
                        else:
                            b = 'unknown_building'

                        if len(c["city"]["name"]["en"]) != 0:
                            ct = c["city"]["name"]["en"]
                        else:
                            ct = 'unknown_city'

                        df_stg.loc[row_index, "location_tree_path"] = f'{ct},{ln},{b}'
                        df_stg.loc[row_index, "coordinates"] = str(c["_geoloc"])
                        df_stg.loc[row_index, "completion_status"] = c["completion_status"]
                        df_stg.loc[row_index, "is_expired"] = not c["active"]
                        df_stg.loc[row_index, "date_insert"] = (datetime.datetime
                                                                .fromtimestamp(c["added"]))
                        df_stg.loc[row_index, "share_url"] = c["short_url"]
                        df_stg.loc[row_index, "listing_nk"] = c["id"]
                        if c["listed_by"] is not None:
                            df_stg.loc[row_index, "listed_by"] = c["listed_by"]["en"]
                        else:
                            df_stg.loc[row_index, "listed_by"] = 'unknown'

                        row_index+=1

                df = pd.concat([df, df_stg])

                print(i)
            # removing duplicates, keeping cleaned data in the same dataframe
            df.drop_duplicates("listing_nk")

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
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")

    run(parser.parse_args())

