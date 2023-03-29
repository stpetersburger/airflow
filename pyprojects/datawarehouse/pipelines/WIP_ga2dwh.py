import argparse
from pyprojects.utils.udfs import *


def run(args):
    project = 'ga'
    pipeline = f"""{project}2dwh"""
    id_pipeline = f"""{pipeline}_{args.btype}"""
    send_telegram_message(1, f"""Pipeline {id_pipeline} has started""")

    ga_dataset = get_creds('gcp', f"""ga_{args.btype}""", 'bq_dataset')

    ga_table_prefix = get_creds('gcp', f"""ga_{args.btype}""", 'bq_ga_table_prefix')

    ga_config_events = get_data_from_googlesheet(args.conn,
                                                 get_creds('gcp', 'gs', 'config_sheet'),
                                                 get_creds('gcp', f"""ga_{args.btype}""", 'ga_config_events')
                                                 )
    ga_config_event_properties = get_data_from_googlesheet(args.conn,
                                                 get_creds('gcp', 'gs', 'config_sheet'),
                                                 get_creds('gcp', f"""ga_{args.btype}""", 'ga_config_event_properties')
                                                           )

    ga_config_events = ga_config_events[ga_config_events.iloc[:, 0] == 1]
    ga_config_event_properties = ga_config_event_properties[ga_config_event_properties.iloc[:, 0] == 1]
    df = pd.DataFrame()

    for index, row in ga_config_events.iterrows():
        if '-' in args.dt:
            from_clause: list = [f"""`{ga_dataset}.{ga_table_prefix}*`"""]
            where_clause: list = [
                f""" AND _TABLE_SUFFIX BETWEEN '{args.dt.split('-')[0]}' AND '{args.dt.split('-')[1]}'"""]
        else:
            from_clause: list = [f"""{ga_dataset}.{ga_table_prefix + args.dt.split(',')[0]}"""]
            where_clause: list = []
        en = row["event_name"]
        print(f"""{en}""")
        # sorting the event_parameters fields in an alphabetic order
        ep = row["event_params"].split('|')
        ep.sort()
        #ep_fields = ',ep.'.join(ep)
        select_clause: list = []
        pivot_index: list = []
        pivot_columns: list = []
        pivot_values: list = []
        for index2, row2 in ga_config_event_properties.iterrows():
            # collecting information on pivoting the final dataframe with the ga_event data
            # to handle UNNEST structure
            if row2["if_pivot_index"] == 1 and (row2["nested_event_name"] == '*'
                                                or en in row2["nested_event_name"]):
                if len(row2["ga_attribute_properties"]) > 0:
                    pivot_index = pivot_index + row2["ga_attribute_properties"].split("|")
                else:
                    pivot_index = pivot_index + row2["ga_attribute_name"].split("|")
            elif row2["if_pivot_index"] == 0 and (row2["nested_event_name"] == '*'
                                                  or en in row2["nested_event_name"]):
                print(row2["ga_attribute_name"])
                pivot_columns = pivot_columns + row2["pivot_columns"].split("|")
                pivot_values = pivot_values + row2["pivot_values"].split("|")

            # creating the query to get the ga_event data from BQ
            if row2["nested_event_name"] == '*' or en in row2["nested_event_name"]:
                if row2["if_unnest"] == 0:
                    en_prop_fields = [row2["ga_attribute_name"]]
                else:
                    print(row2["ga_attribute_name"])
                    if len(row2["unnest_alias"]) > 0:
                        # means the event property is a record, which needs to be unnested on a query level
                        # adding the clause to the query
                        from_clause = from_clause + \
                                      [f"""UNNEST ({row2['ga_attribute_name']}) {row2['unnest_alias']}"""]
                        # adding the fields to the query
                        v = [f"""{row2['unnest_alias']}.""" + x for x in row2["ga_attribute_properties"].split('|')]
                        coalesce_flds: list = []
                        for fld in v:
                            if 'value' in fld:
                                if 'int' in fld or 'double' in fld:
                                    coalesce_flds = coalesce_flds + [f""" CAST({fld} as string)"""]
                                else:
                                    coalesce_flds = coalesce_flds + [fld]

                            else:
                                en_prop_fields = en_prop_fields + [fld]

                        if len(coalesce_flds) > 0:
                            en_prop_fields = en_prop_fields + [f"""COALESCE({','.join(coalesce_flds)}) ep_value"""]

                    else:
                        en_prop_fields = [f"""{row2['ga_attribute_name']}.""" + x
                                          for x in row2["ga_attribute_properties"].split('|')]

            select_clause = select_clause + en_prop_fields
            en_prop_fields = []

        query = "SELECT " + ",".join(select_clause) + \
                " FROM " + ','.join(from_clause) + \
                " WHERE event_name='" + en + "'" + ''.join(where_clause)
        print(query)
        # showing the event parameters to bring into table
        print(ep)
        df = get_from_gbq(args.conn, str_sql=query, etl_desc=id_pipeline, note=f"""{en}""")
        # showing the list of available event parameters
        print(df.ep_key.unique())
        if not df.empty:
            # filtering the vent parameters, which are needed to be brought into table
            df = df[df['ep_key'].isin(ep)]
            df = df.drop_duplicates()
            # adding dataframe index as a column for pivoting the data later to enhance
            # the uniqueness of the pivot_index
            df.index.name = 'rownum'
            pivot_index = ['rownum'] + pivot_index
            df = df.reset_index()
            # printing pivoting parameters
            print(pivot_index)
            print(pivot_columns)
            print(pivot_values)
            # pivoting the data
            df = df.pivot(index=pivot_index, columns=pivot_columns, values=pivot_values)
            # dropping pivot "value"
            df.columns = df.columns.droplevel(0)
            # dropping pivot "key"
            df.columns.name = None
            # transforming index into columns
            df = df.rename_axis(pivot_index).reset_index()
            # removing issues with naming
            df = clean_pandas_dataframe(df, pipeline='', standartise=True)
            df = clean_pandas_dataframe(df, pipeline)
            # dropping the rownum column
            df = df.drop('rownum', axis=1)
            df = df.drop_duplicates()
            write_to_gbq(args.conn, 'gcp_ga', en, df, 'append')
            df = pd.DataFrame()
        else:
            df = pd.DataFrame()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with google analytics')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-business_type', dest='btype', required=True,
                        help="b2c or b2b")
    parser.add_argument('-date', dest='dt', required=False,
                        help="start date to get the data")
    run(parser.parse_args())
