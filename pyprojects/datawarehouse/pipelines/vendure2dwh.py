"""
Script to bring pgdata from Spryker to Analytics datalake
"""

import argparse
from pyprojects.utils.udfs import *


def run(args):
    project = 'vendure'
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

    df_new_items = pd.DataFrame()
    df_new_orders = pd.DataFrame()
    df_hist_items = pd.DataFrame()
    last_modified = 0
    cnt = 0

    # fraud or test check
    # not all the users have fk_customer
    fraudtestemails = get_gbq_dim_data(args.conn,
                                       'gcp_gs',
                                       'test_fraud_users',
                                       'customer_reference',
                                       args.btype).values.tolist()
    fraudtestemails = list(map(''.join, fraudtestemails))
    fraudtestemails = [x.strip().lower() for x in fraudtestemails]

    for p in prefix:
        for obj in etl_bucket.objects.filter(Prefix=p):
            if obj.key.endswith('json') and calendar.timegm(obj.last_modified.timetuple()) > delta:
                print(obj.key)
                # get the message data
                msg_data = json.loads(obj.get()['Body'].read().decode('utf-8'))['order']
                cnt += 1
                # to keep track of the files read
                if calendar.timegm(obj.last_modified.timetuple()) >= last_modified:
                    last_modified = calendar.timegm(obj.last_modified.timetuple())

                ### VENDURE LOGIC

                # items_statuses
                sqlstr = f"""SELECT  fk_sales_order_item_state,
                                     engine_order_state_name_en
                               FROM  gcp_gs.map_order_item_status WHERE engine_name_en='{project}'"""
                df_dim_item_states = get_from_gbq(args.conn, sqlstr, project, pipeline)

                msg_item_states_order_info = pd.json_normalize(msg_data["meta"])[get_etl_schema(id_pipeline,
                                                                                                'items_state_order_info',
                                                                                                'fields')]

                msg_item_states_order_info.rename(columns=get_etl_schema(id_pipeline, 'items_state_order_info',
                                                                                     'rename'), inplace=True)


                msg_data_item_states = pd.json_normalize(msg_data["items"])[get_etl_schema(id_pipeline,
                                                                                                'items_state',
                                                                                                'fields')]
                msg_data_item_states.rename(columns=get_etl_schema(id_pipeline, 'items_state',
                                                                                     'rename'), inplace=True)
                msg_data_item_states = (msg_data_item_states
                                        .join(msg_item_states_order_info, how='cross')
                                        .join(df_dim_item_states.set_index('engine_order_state_name_en'), on='engine_order_state_name_en')
                                        .drop(columns=get_etl_schema(id_pipeline, 'items_state', 'drop'),axis = 1)
                                        )

                df_hist_items = concatenate_dataframes(df_hist_items, msg_data_item_states)

                # orders and items
                if msg_item_states_order_info['engine_order_state_name_en'][0] == 'PaymentSettled':
                    # orders
                    msg_data_order = pd.json_normalize(msg_data['meta'])[get_etl_schema(id_pipeline,
                                                                                                'order',
                                                                                                'fields')]
                    msg_data_order_custom_info = pd.json_normalize(msg_data['meta']['customFields'])[get_etl_schema(id_pipeline,
                                                                                                'order_meta_customfields',
                                                                                                'fields')]
                    msg_data_order_customer = pd.json_normalize(msg_data['customer'])[get_etl_schema(id_pipeline,
                                                                                                'order_customer',
                                                                                                'fields')]
                    if msg_data['customer']['user'] is not None:
                        msg_data_order_customer_user = pd.json_normalize(msg_data['customer']['user'])[get_etl_schema(id_pipeline,
                                                                                                    'order_customer_user',
                                                                                                    'fields')]


                    msg_data_order_customer_user.rename(columns=get_etl_schema(id_pipeline,'order_customer_user',
                                                                                                'rename'), inplace=True)
                    msg_data_order_customer_custom_fields = pd.json_normalize(msg_data['customer']['customFields'])[get_etl_schema(id_pipeline,
                                                                                                'order_customer_custom_fields',
                                                                                                'fields')]
                    msg_data_order_shipping_address = pd.json_normalize(msg_data['shippingAddress'])[get_etl_schema(id_pipeline,
                                                                                                    'order_shippingAddress',
                                                                                                    'fields')]
                    msg_data_order_totals = pd.json_normalize(msg_data['totals'][0])[get_etl_schema(id_pipeline,
                                                                                                    'order_totals',
                                                                                                    'fields')]

                    msg_data_order = msg_data_order.join(msg_data_order_custom_info).join(msg_data_order_customer).join(
                        msg_data_order_customer_user).join(msg_data_order_customer_custom_fields).join(
                        msg_data_order_shipping_address).join(msg_data_order_totals)

                    msg_data_order.rename(columns=get_etl_schema(id_pipeline, 'order',
                                                                              'rename'), inplace=True)
                    if msg_data_order["customer_reference"][0] is not None:
                        customer_to_check = msg_data_order["customer_reference"][0].strip().lower()
                    else:
                        customer_to_check = 'undefined'

                    if customer_to_check.strip().lower() in fraudtestemails:
                        msg_data_order["is_test"] = True
                    else:
                        msg_data_order["is_test"] = False

                    df_new_orders = concatenate_dataframes(df_new_orders, msg_data_order)
                    # items
                    for itm in msg_data['items']:

                        msg_data_item = pd.json_normalize(itm)[get_etl_schema(id_pipeline,
                                                                                        'items',
                                                                                        'fields')]
                        d_amount = 0
                        d_source = ''

                        for d in itm['discounts']:
                            d_amount = d_amount + d['amount']
                            if len(d_source) > 0 and len(d['adjustmentSource']) > 0:
                                d_source = '|' + d_source + d['adjustmentSource']
                            elif len(d['adjustmentSource']) > 0:
                                d_source = d_source + d['adjustmentSource']

                        msg_data_item_discount = pd.DataFrame({"discount_amount": [d_amount],
                                                               "discount_type": [d_source]})

                        msg_data_item = (msg_data_item.join(msg_item_states_order_info)
                                                      .join(msg_data_item_discount)
                                                      .drop(columns=get_etl_schema(id_pipeline,
                                                                                     'items',
                                                                                     'drop'), axis=1))
                        msg_data_item.rename(columns=get_etl_schema(id_pipeline, 'items',
                                                                                   'rename'), inplace=True)
                        df_new_items = concatenate_dataframes(df_new_items, msg_data_item)

    if cnt > 0:

        # create the offset delta info tp be written into datawarehouse
        delta_update = {"id_pipeline": f"""{id_pipeline}""", "delta": last_modified}
        delta_update = pd.DataFrame.from_dict([delta_update])

        # prepare datasets to be written into datawarehouse
        df_new_orders = clean_pandas_dataframe(df_new_orders.drop_duplicates(), '', True, last_modified)
        df_new_items = clean_pandas_dataframe(df_new_items.drop_duplicates(), '', True, last_modified)
        df_hist_items = clean_pandas_dataframe(df_hist_items.drop_duplicates(), '', True, last_modified)
        delta_update = clean_pandas_dataframe(delta_update.drop_duplicates(), pipeline)

        # write datasets into datawraehouse, using incremental approach
        write_to_gbq(args.conn, args.schema, f"""{args.btype}_sales_orders_{project}""",
                     clean_pandas_dataframe(df_new_orders, pipeline,'', last_modified), args.wtype)
        write_to_gbq(args.conn, args.schema, f"""{args.btype}_sales_order_items_{project}""",
                     clean_pandas_dataframe(df_new_items, pipeline,'', last_modified), args.wtype)
        write_to_gbq(args.conn, args.schema, f"""{args.btype}_sales_order_item_states_{project}""",
                     clean_pandas_dataframe(df_hist_items, pipeline,'', last_modified), args.wtype)

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