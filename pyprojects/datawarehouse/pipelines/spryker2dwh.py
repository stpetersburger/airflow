"""
Script to bring pgdata from Spryker to Analytics datalake
"""

import argparse
from pyprojects.utils.udfs import *
import boto3
import pandas as pd


def run(args):
    project = 'spryker'
    pipeline = f"""{project}2dwh"""
    id_pipeline = f"""{pipeline}_{args.btype}"""

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
    fraudtestemails = get_gbq_dim_data(args.conn, 'gcp_gs', 'test_fraud_users', 'email').values.tolist()
    fraudtestemails = list(map(''.join, fraudtestemails))
    fraudtestemails = [x.strip().lower() for x in fraudtestemails]

    for p in prefix:
        for obj in etl_bucket.objects.filter(Prefix=p):
            if obj.key.endswith('json') and calendar.timegm(obj.last_modified.timetuple()) > delta:
                cnt += 1
                print(obj.key)

                if calendar.timegm(obj.last_modified.timetuple()) >= last_modified:
                    last_modified = calendar.timegm(obj.last_modified.timetuple())

                msg_data = json.loads(obj.get()['Body'].read().decode('utf-8'))

                msg_data_items_state = pd.DataFrame.from_dict(msg_data["items"])[get_etl_schema(pipeline, 'items_state',
                                                                                                'fields')]

                df_hist_items = concatenate_dataframes(df_hist_items, msg_data_items_state)

                if msg_data["eventName"] == '' and msg_data["items"][0]["item_status"] == 'new':


                    msg_data_order = clean_pandas_dataframe(pd.DataFrame.from_dict([msg_data["order"]]))[get_etl_schema(
                                                                                                         pipeline,
                                                                                                         'order',
                                                                                                         'fields')]

                    msg_data_order["customer_created_at"] = msg_data["customer"]["created_at"]

                    email_to_check = msg_data["customer"]["email"].strip().lower()
                    if email_to_check.strip().lower() in fraudtestemails:
                        msg_data_order["is_test"] = True

                    msg_data_order_totals = clean_pandas_dataframe(
                        pd.DataFrame.from_dict([msg_data["order_totals"][0]]), '', True)

                    msg_data_order_totals = msg_data_order_totals.drop(columns=get_etl_schema(pipeline, 'order_totals',
                                                                                                        'drop'), axis=1)

                    msg_data_shipping_expense = clean_pandas_dataframe(pd.DataFrame.from_dict(
                        [msg_data["shipping_expense"][0]]), '', True)[get_etl_schema(pipeline,
                                                                                     'shipping_expense',
                                                                                     'fields')]
#
                    msg_data_shipping_address = \
                        clean_pandas_dataframe(pd.DataFrame.from_dict([msg_data["shipping_address"]]),
                                               '',
                                               True)[get_etl_schema(pipeline,
                                                                    'shipping_address',
                                                                    'fields')]

                    msg_data_order = msg_data_order.join(msg_data_order_totals) \
                        .join(msg_data_shipping_expense) \
                        .join(msg_data_shipping_address).drop(columns=get_etl_schema(pipeline,
                                                                                     'order',
                                                                                     'drop'),
                                                              axis=1)

                    msg_data_order = msg_data_order.filter(items=get_etl_schema(pipeline,
                                                                                'order',
                                                                                'filter'))

                    df_news_orders = concatenate_dataframes(df_news_orders, msg_data_order)

                    msg_data_items = msg_data["items"]

                    for el in msg_data_items:
                        df_el = clean_pandas_dataframe(pd.DataFrame.from_dict([el])[get_etl_schema(pipeline,
                                                                                                   'items',
                                                                                                   'fields')])
                        df_news_items = concatenate_dataframes(df_news_items, df_el)

    if cnt > 0:
        df_news_orders = clean_pandas_dataframe(df_news_orders.drop_duplicates())
        df_news_orders = clean_pandas_dataframe(df_news_orders, pipeline, '', last_modified)
        df_news_items = clean_pandas_dataframe(df_news_items.drop_duplicates(), pipeline, '', last_modified)

        df_news_orders.rename(columns=get_etl_schema(pipeline, 'order', 'rename'), inplace=True)
        df_news_items.rename(columns=get_etl_schema(pipeline, 'items', 'rename'), inplace=True)

        if not df_hist_items.empty:
            df_hist_items.rename(columns=get_etl_schema(pipeline, 'items_state', 'rename'), inplace=True)
            df_hist_items = get_deduplication_data(args.conn,
                                                   f"""{pipeline}_items""",
                                                   df_hist_items,
                                                   get_etl_schema(pipeline, 'items_state', 'deduplication'))
            df_hist_items = clean_pandas_dataframe(df_hist_items.drop_duplicates(), pipeline, '', last_modified)

        delta_update = {"id_pipeline": f"""{id_pipeline}""", "delta": last_modified}
        delta_update = pd.DataFrame.from_dict([delta_update])
        delta_update = clean_pandas_dataframe(delta_update.drop_duplicates(), pipeline)

        try:
            write_to_gbq(args.conn,
                         args.schema, 'sales_orders', df_news_orders, args.wtype)
            send_telegram_message(
                1, f"""BQ spryker2dwh sales_orders""")
            write_to_gbq(args.conn,
                         args.schema, 'sales_order_items', df_news_items, args.wtype)
            send_telegram_message(
                1, f"""BQ spryker2dwh sales_order_items""")
            write_to_gbq(args.conn,
                         args.schema, 'sales_order_item_states', df_hist_items, args.wtype)
            send_telegram_message(
                1, f"""BQ spryker2dwh sales_order_item_states""")
            write_to_gbq(args.conn,
                         'etl_metadata', 'airflow_run', clean_pandas_dataframe(delta_update, pipeline), args.wtype)
            send_telegram_message(
                1, f"""BQ spryker2dwh updated airflow run metadata: number of files - {cnt}, delta - {last_modified}""")
        except Exception as e:
            print(f'caught {type(e)}: {str(e)}')
            send_telegram_message(0, f"""BQ spryker2dwh caught {type(e)}: {str(e)}""")


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
