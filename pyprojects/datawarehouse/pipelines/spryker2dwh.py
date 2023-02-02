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
    pipeline_nk = f"""{pipeline}_{args.btype}"""

    # s3 credentials
    s3 = boto3.resource(
        service_name='s3',
        region_name=get_creds(args.schema, pipeline, 'region_name'),
        aws_access_key_id=get_creds(args.schema, pipeline, 'aws_access_key_id'),
        aws_secret_access_key=get_creds(args.schema, pipeline, 'aws_secret_access_key')
    )

    delta = get_delta(args.conn, pipeline_nk, args.dt)

    my_bucket = s3.Bucket(get_creds(args.schema, pipeline, 'bucket'))
    prefix = get_s3_prefix(project, args.btype, args.dt)
    print(prefix)

    df_news_items = pd.DataFrame()
    df_news_orders = pd.DataFrame()
    df_hist_items = pd.DataFrame()
    df_hist_orders = pd.DataFrame()
    last_modified = 0
    cnt = 0

    for p in prefix:
        for obj in my_bucket.objects.filter(Prefix=p):
            if obj.key.endswith('json') and time.mktime(obj.last_modified.timetuple()) > delta:
                cnt += 1
                print(obj.key)

                if time.mktime(obj.last_modified.timetuple()) >= last_modified:
                    last_modified = time.mktime(obj.last_modified.timetuple())

                msg_data = json.loads(obj.get()['Body'].read().decode('utf-8'))

                msg_data_order = clean_pandas_dataframe(pd.DataFrame.from_dict([msg_data["order"]]))[[
                    'id_sales_order',
                    'is_test',
                    'order_reference',
                    'fk_locale',
                    'cart_note',
                    'currency_iso_code',
                    'price_mode',
                    'order_exchange_rate',
                    'fk_customer',
                    'order_custom_reference',
                    'customer_reference',
                    'oms_processor_identifier',
                    'created_at',
                    'updated_at'
                ]]

                if msg_data["eventName"] == '' and msg_data["items"][0]["item_status"] == 'new':

                    msg_data_order_status = msg_data_order.filter(items=[
                        'id_sales_order',
                        'updated_at'])
                    msg_data_order_status['order_status_name_en'] = "new"
                    msg_data_order_status = msg_data_order_status.filter(items=[
                        'id_sales_order',
                        'order_status_name_en',
                        'updated_at'])

                    df_hist_orders = concatenate_dataframes(df_hist_orders, msg_data_order_status)

                    msg_data_order_totals = clean_pandas_dataframe(
                        pd.DataFrame.from_dict([msg_data["order_totals"][0]]))
                    msg_data_order_totals = msg_data_order_totals.drop(columns=["created_at", "updated_at"], axis=1)

                    msg_data_shipping_expense = clean_pandas_dataframe(pd.DataFrame.from_dict(
                        [msg_data["shipping_expense"][0]]))[[
                        'id_sales_expense',
                        'discount_amount_aggregation',
                        'gross_price',
                        'name',
                        'net_price',
                        'price',
                        'price_to_pay_aggregation',
                        'refundable_amount',
                        'tax_amount'
                    ]]

                    msg_data_shipping_address = \
                        clean_pandas_dataframe(pd.DataFrame.from_dict([msg_data["shipping_address"]]))[[
                            'id_sales_order_address',
                            'fk_country',
                            'fk_region',
                            'address1',
                            'address2',
                            'address3'
                        ]]

                    msg_data_order = msg_data_order.join(msg_data_order_totals) \
                        .join(msg_data_shipping_expense) \
                        .join(msg_data_shipping_address)

                    df_news_orders = concatenate_dataframes(df_news_orders, msg_data_order)

                    msg_data_items = msg_data["items"]

                    for el in msg_data_items:
                        df_el = clean_pandas_dataframe(pd.DataFrame.from_dict([el])[[
                            'id_sales_order_item',
                            'fk_sales_order',
                            'fk_sales_order_item_bundle',
                            'fk_sales_shipment',
                            'merchant_id',
                            'group_key',
                            'sku',
                            'quantity',
                            'is_quantity_splittable',
                            'canceled_amount',
                            'discount_amount_aggregation',
                            'discount_amount_full_aggregation',
                            'gross_price',
                            'net_price',
                            'price',
                            'price_to_pay_aggregation',
                            'product_offer_reference',
                            'refundable_amount',
                            'product_option_price_aggregation',
                            'subtotal_aggregation',
                            'tax_amount',
                            'tax_amount_full_aggregation',
                            'created_at',
                            'updated_at'
                        ]]
                                                       )
                        df_news_items = concatenate_dataframes(df_news_items, df_el)
                else:
                    # items_hist
                    msg_data_items_state = pd.DataFrame.from_dict(msg_data["items"])[["id_sales_order_item",
                                                                                      "fk_sales_order",
                                                                                      "fk_oms_order_item_state",
                                                                                      "updated_at"]]

                    df_hist_items = concatenate_dataframes(df_hist_items, msg_data_items_state)

                    # orders_hist
                    if msg_data["eventName"] != '':
                        msg_data_order_status = msg_data_order.filter(items=[
                            'id_sales_order',
                            'updated_at'])
                        msg_data_order_status['order_status_name_en'] = msg_data["eventName"]
                        msg_data_order_status = msg_data_order_status.filter(items=[
                            'id_sales_order',
                            'order_status_name_en',
                            'updated_at'])
                        df_hist_orders = concatenate_dataframes(df_hist_orders, msg_data_order_status)
                        
    if cnt > 0:

        delta_update = {"pipeline_nk": f"""{pipeline_nk}""", "delta": last_modified}
        delta_update = pd.DataFrame.from_dict([delta_update])

        df_news_orders = clean_pandas_dataframe(df_news_orders.drop_duplicates(), pipeline)
        df_hist_orders = clean_pandas_dataframe(df_hist_orders.drop_duplicates(), pipeline)
        df_news_items = clean_pandas_dataframe(df_news_items.drop_duplicates(), pipeline)
        df_hist_items = clean_pandas_dataframe(df_hist_items.drop_duplicates(), pipeline)
        delta_update = clean_pandas_dataframe(delta_update.drop_duplicates(), pipeline)

        #deduplication with history:
        if not df_hist_orders.empty:
            df_hist_orders = get_deduplication_data(args.conn,
                                                    f"""{pipeline_nk}_orders""",
                                                    df_hist_orders,
                                                    ['id_sales_order', 'updated_at'])
        if not df_hist_items.empty:
            df_hist_items = get_deduplication_data(args.conn,
                                                   f"""{pipeline_nk}_items""",
                                                   df_hist_items,
                                                   ['id_sales_order_item', 'updated_at'])
#
        print(f'WRITE TO BQ START - {datetime.datetime.now()}')

        try:
            write_to_gbq(args.conn,
                         args.schema,
                         'sales_orders',
                         df_news_orders,
                         args.wtype)
            write_to_gbq(args.conn,
                         args.schema,
                         'sales_order_states',
                         df_hist_orders,
                         args.wtype)
            write_to_gbq(args.conn,
                         args.schema,
                         'sales_order_items',
                         df_news_items,
                         args.wtype)
            write_to_gbq(args.conn,
                         args.schema,
                         'sales_order_item_states',
                         df_hist_items,
                         args.wtype)
            write_to_gbq(args.conn,
                         'etl_metadata',
                         'airflow_run',
                         clean_pandas_dataframe(delta_update, 'spryker2dwh'),
                         args.wtype)

            print(f'WRITE TO BQ END - {datetime.datetime.now()}')
        except Exception as e:
            print(f'caught {type(e)}: {str(e)}')
            sys.exit()


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
