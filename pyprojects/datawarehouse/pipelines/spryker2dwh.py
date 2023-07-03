"""
Script to bring pgdata from Spryker to Analytics datalake
"""

import argparse
from pyprojects.utils.udfs import *


def run(args):
    project = 'spryker'
    pipeline = f"""{project}2dwh"""
    id_pipeline = f"""{pipeline}_{args.btype}"""

    send_telegram_message(1, f"""Pipeline {id_pipeline} has started""")

    # s3 credentials
    etl_s3 = boto3.resource(
        service_name='s3',
        region_name=get_creds(args.schema, id_pipeline, 'region_name'),
        aws_access_key_id=get_creds(args.schema, id_pipeline, 'aws_access_key_id'),
        aws_secret_access_key=get_creds(args.schema, id_pipeline, 'aws_secret_access_key')
    )

    delta = get_delta(args.conn, id_pipeline, args.dt)
    print(delta)

    etl_bucket = etl_s3.Bucket(get_creds(args.schema, id_pipeline, 'bucket'))
    prefix = get_s3_prefix(project, args.btype, args.dt)
    print(prefix)

    df_news_items = pd.DataFrame()
    df_news_orders = pd.DataFrame()
    df_hist_items = pd.DataFrame()
    last_modified = 0
    cnt = 0

    # fraud or test check
    # not all the users have fk_customer
    fraudtestemails = get_gbq_dim_data(args.conn, 'gcp_gs', 'test_fraud_users', 'customer_reference').values.tolist()
    fraudtestemails = list(map(''.join, fraudtestemails))
    fraudtestemails = [x.strip().lower() for x in fraudtestemails]

    for p in prefix:
        for obj in etl_bucket.objects.filter(Prefix=p):
            if obj.key.endswith('json') and calendar.timegm(obj.last_modified.timetuple()) > delta:
                print(obj.key)
                # get the message data
                msg_data = json.loads(obj.get()['Body'].read().decode('utf-8'))
                cnt += 1
                # to keep track of the files read
                if calendar.timegm(obj.last_modified.timetuple()) >= last_modified:
                    last_modified = calendar.timegm(obj.last_modified.timetuple())

                # get the fields, needed for items_states from etl_schemas.json
                msg_data_items_state = pd.DataFrame.from_dict(msg_data["items"])[get_etl_schema(id_pipeline,
                                                                                                'items_state',
                                                                                                'fields')]
                # append (concatenate) items_state data from this message with the data of the other messages read
                df_hist_items = concatenate_dataframes(df_hist_items, msg_data_items_state)

                # if message has no event name, but the items are new, then the order is new
                if ((msg_data["items"][0]["fk_oms_order_process"] == 4 and msg_data["eventName"] == '' and
                    msg_data["items"][0]["fk_oms_order_item_state"] == 1) or
                    (msg_data["eventName"] == 'place' and args.btype == 'b2b') or
                    (msg_data["items"][0]["fk_oms_order_process"] == 2 and msg_data["eventName"] == '' and
                     msg_data["items"][0]["fk_oms_order_item_state"] == 5)):

                    msg_data_order = pd.DataFrame.from_dict([msg_data["order"]])[get_etl_schema(id_pipeline, 'order',
                                                                                                              'fields')]
                    # assign value from the customer section of the message
                    msg_data_order["customer_created_at"] = msg_data["customer"]["created_at"]

                    # adding discount codes of influencers for b2c into cart_note field
                    for el in msg_data:
                        if el == "discounts":
                            msg_data_order["cart_note"] = '|'.join(msg_data["discounts"])
                        # change 29.03.2023 - adding loyalty information
                        if el == "loyalty":
                            msg_data_loyalty = clean_pandas_dataframe(pd.DataFrame.from_dict(
                                [msg_data["loyalty"]]), '', True)[get_etl_schema(id_pipeline, 'loyalty', 'fields')]
                            msg_data_order = msg_data_order.join(msg_data_loyalty)

                    msg_data_order = clean_pandas_dataframe(msg_data_order)
                    # for b2c the fraud check needs to be done as of test/fraud users
                    customer_to_check = msg_data["customer"]["customer_reference"].strip().lower()
                    if customer_to_check.strip().lower() in fraudtestemails:
                        msg_data_order["is_test"] = True

                    # get data of order_totals section of the message and clean the fields' naming
                    msg_data_order_totals = clean_pandas_dataframe(
                        pd.DataFrame.from_dict([msg_data["order_totals"][0]]), '', True)
                    # columns to drop taken from etl_schemas.json
                    msg_data_order_totals = msg_data_order_totals.drop(columns=get_etl_schema(id_pipeline,
                                                                                              'order_totals', 'drop'),
                                                                       axis=1)

                    msg_data_shipping_expense = clean_pandas_dataframe(pd.DataFrame.from_dict(
                        [msg_data["shipping_expense"][0]]), '', True)[get_etl_schema(id_pipeline,
                                                                                     'shipping_expense',
                                                                                     'fields')]

                    if not msg_data["shipping_address"] == "":
                        msg_data_shipping_address = clean_pandas_dataframe(pd.DataFrame.from_dict(
                                                                        [msg_data["shipping_address"]]),
                                                                        '',
                                                                        True)[get_etl_schema(id_pipeline,
                                                                                             'shipping_address',
                                                                                             'fields')]
                    else:
                        msg_data_shipping_address = pd.DataFrame()

                    msg_data_order = msg_data_order.join(msg_data_order_totals) \
                        .join(msg_data_shipping_expense) \
                        .join(msg_data_shipping_address).drop(columns=get_etl_schema(id_pipeline, 'order', 'drop'),
                                                              axis=1)

                    # using filter method, change the order of fields in dataframe
                    msg_data_order = msg_data_order.filter(items=get_etl_schema(id_pipeline, 'order', 'filter'))

                    df_news_orders = concatenate_dataframes(df_news_orders, msg_data_order)

                    msg_data_items = msg_data["items"]
                    # as the status of the items is new, it's data goes to the fact_items
                    for el in msg_data_items:
                        df_el = clean_pandas_dataframe(pd.DataFrame.from_dict([el])[get_etl_schema(id_pipeline, 'items',
                                                                                                   'fields')])
                        df_news_items = concatenate_dataframes(df_news_items, df_el)
    if cnt > 0:
        df_news_orders.rename(columns=get_etl_schema(id_pipeline, 'order', 'rename'), inplace=True)
        df_news_items.rename(columns=get_etl_schema(id_pipeline, 'items', 'rename'), inplace=True)
        df_hist_items.rename(columns=get_etl_schema(id_pipeline, 'items_state', 'rename'), inplace=True)

        # deduplicate items states with existing in datawarehouse already (in case of data dump)
        df_hist_items = get_deduplication_data(args.conn, args.btype, f"""{pipeline}_items""", df_hist_items,
                                               get_etl_schema(id_pipeline, 'items_state', 'deduplication'))

        # create the offset delta info tp be written into datawarehouse
        delta_update = {"id_pipeline": f"""{id_pipeline}""", "delta": last_modified}
        delta_update = pd.DataFrame.from_dict([delta_update])

        # prepare datasets to be written into datawarehouse
        df_news_orders = clean_pandas_dataframe(df_news_orders.drop_duplicates(), pipeline, '', last_modified)
        df_news_items = clean_pandas_dataframe(df_news_items.drop_duplicates(), pipeline, '', last_modified)
        df_hist_items = clean_pandas_dataframe(df_hist_items.drop_duplicates(), pipeline, '', last_modified)
        delta_update = clean_pandas_dataframe(delta_update.drop_duplicates(), pipeline)

        # write datasets into datawraehouse, using incremental approach
        write_to_gbq(args.conn, args.schema, f"""{args.btype}_sales_orders""", df_news_orders, args.wtype)
        write_to_gbq(args.conn, args.schema, f"""{args.btype}_sales_order_items""", df_news_items, args.wtype)
        write_to_gbq(args.conn, args.schema, f"""{args.btype}_sales_order_item_states""", df_hist_items, args.wtype)

        write_to_gbq(args.conn, 'etl_metadata', 'airflow_run', delta_update, 'append')

    send_telegram_message(1, f"""Pipeline {id_pipeline} has finished. {cnt} files; delta - {last_modified} """)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with spryker')
    parser.add_argument('-business_type', dest='btype', required=True,
                        help="b2c or b2b")
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    parser.add_argument('-writing_type', dest='wtype', required=True,
                        help="append or replace")
    parser.add_argument('-date', dest='dt', required=False,
                        help="start date to get the data")
    run(parser.parse_args())