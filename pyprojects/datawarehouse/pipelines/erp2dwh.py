"""
Script to bring pgdata from Salesforce to Analytics datalake
"""

import argparse, math

import numpy as np

from pyprojects.utils.udfs import *
from simple_salesforce import Salesforce
import requests
import pandas as pd
from io import StringIO
from collections import OrderedDict
from collections import OrderedDict


def run(args):

    project = 'erp'
    pipeline = f"""{args.schema}_{args.btype}"""

    i_url = get_creds(project, pipeline, 'url')
    un = get_creds(project, pipeline, 'username')
    pw = get_creds(project, pipeline, 'password')
    oi = get_creds(project, pipeline, 'organisation_id')
    ins = get_creds(project, pipeline, 'instance_id')
    dm = get_creds(project, pipeline, 'domain')

    sf = Salesforce(instance_url=i_url,
                    username=un,
                    password=pw,
                    organizationId=oi,
                    instance=ins,
                    domain=dm)

    for tbl in etl_schemas[pipeline]:

        print(tbl)

        flds_base = get_etl_schema(pipeline, tbl, 'base_attributes')
        print(flds_base)

        if len(flds_base) > 2:
            run_type = 'full'
            delta_updated = get_delta(args.conn, f"""{args.schema}_{tbl}_u""")
            delta_created = get_delta(args.conn, f"""{args.schema}_{tbl}_c""")
            print(delta_created)
            print(delta_updated)

        else:
            run_type = 'new_only'
            delta_created = get_delta(args.conn, f"""{args.schema}_{tbl}""")
            delta_updated = -1

        if isinstance(delta_created, numbers.Number):
            tbl_attr = sf.query_all(f"""SELECT  COUNT({flds_base[0]}) cnt FROM  {tbl}""")
            delta_created = datetime.datetime.now(timezone.utc) - datetime.timedelta(days=5*365)
            print(delta_created)
            delta_updated = -1

            wtype='replace'
        else:
            str_sql = ''
            wtype = 'append'
            if run_type == 'new_only' or isinstance(delta_updated, numbers.Number):
                str_sql = f"""DELETE  
                                FROM  {args.schema}.{tbl} 
                               WHERE  {flds_base[1]} >= '{delta_created.isoformat()}'"""
                delta_updated = -1
            else:
                str_sql = f"""DELETE  
                                FROM  {args.schema}.{tbl} 
                               WHERE  {flds_base[1]} >= '{delta_created.isoformat()}'"""

            execute_gbq(args.conn, str_sql, etl_desc='', note='')

            if delta_updated == -1:
                tbl_attr = sf.query_all(f"""SELECT  COUNT({flds_base[0]}) cnt 
                                              FROM  {tbl}
                                             WHERE  {flds_base[1]} >= {delta_created.isoformat()}""")
            else:
                tbl_attr = sf.query_all(f"""SELECT  COUNT_DISTINCT({flds_base[0]}) cnt 
                                              FROM  {tbl}
                                             WHERE  {flds_base[1]} >= {delta_created.isoformat()} 
                                                    OR {flds_base[2]} >= {delta_updated.isoformat()} """)



        num_of_new_rows = math.ceil(tbl_attr['records'][0]['cnt'])

        deduplicaton_coeff = math.ceil(0.75 * num_of_new_rows / 6000)
        print(deduplicaton_coeff)

        iteration = 1

        print(num_of_new_rows)

        flds = get_etl_schema(pipeline, tbl,'fields')
        print(flds)
        df = pd.DataFrame()

        d = delta_created.isoformat()
        dd = d

        print(d)
        print(type(d))
        print(dd)

        while len(df.index) < num_of_new_rows:
            print(len(df.index))
            print(iteration)
            if len(flds_base) == 2:
                results = sf.query_all(f"""SELECT  {','.join(flds)} 
                                             FROM  {tbl} 
                                            WHERE  {flds_base[1]} >= {d} 
                                            ORDER  BY {flds_base[1]} LIMIT 2000""")
            else:
                results = sf.query_all(f"""SELECT  {','.join(flds)} 
                                             FROM  {tbl} 
                                            WHERE  {flds_base[1]} >= {d} OR  {flds_base[2]} >= {dd} 
                                            ORDER  BY {flds_base[1]} LIMIT 2000""")


            r = results['records']
            for el in r:
                dl = []
                for k, v in el.items():
                    if isinstance(v, OrderedDict):
                        dl.append(k)

                for el_dl in dl:
                    el.pop(el_dl)

                df = pd.concat([df, pd.DataFrame(el, index=[0])])

            if iteration % deduplicaton_coeff == 0 and iteration > 0:
                print(len(df.index))
                df = df.drop_duplicates()
                print('deduplicated')
                print(len(df.index))

            d = df[flds_base[1]].max()
            if len(flds_base) > 2:
                dd = df[flds_base[2]].max()

            iteration += 1
        df = clean_pandas_dataframe(df.drop_duplicates(), '', False)
        print(df.drop_duplicates())

        try:
            write_to_gbq(args.conn, args.schema, dataset=tbl,
                         dataframe=clean_pandas_dataframe(df, args.schema), wtype=wtype)
            delta_update ={}
            if len(flds_base) == 2:
                delta_update = {"id_pipeline": f"""{args.schema}_{tbl}""", "delta": d}
                delta_update = pd.DataFrame.from_dict([delta_update])
                delta_update["delta"] = pd.to_datetime(delta_update["delta"])

                write_to_gbq(args.conn, 'etl_metadata', 'airflow_run', delta_update, 'append')
            else:
                delta_update = {"id_pipeline": f"""{args.schema}_{tbl}_c""", "delta": d}
                delta_update = pd.DataFrame.from_dict([delta_update])
                delta_update["delta"] = pd.to_datetime(delta_update["delta"])

                write_to_gbq(args.conn, 'etl_metadata', 'airflow_run', delta_update, 'append')

                delta_update = {"id_pipeline": f"""{args.schema}_{tbl}_u""", "delta": dd}
                delta_update = pd.DataFrame.from_dict([delta_update])
                delta_update["delta"] = pd.to_datetime(delta_update["delta"])

                write_to_gbq(args.conn, 'etl_metadata', 'airflow_run', delta_update, 'append')

        except Exception as e:
            send_telegram_message(0, f' {pipeline} caught {type(e)}: {str(e)}')
            print(f'caught {type(e)}: {str(e)}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with erp')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    parser.add_argument('-business_type', dest='btype', required=True,
                        help="b2c or b2b")
    parser.add_argument('-date', dest='dt', required=False,
                        help="start date to get the data")
    run(parser.parse_args())
