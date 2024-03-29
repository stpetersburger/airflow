"""
Script to bring pgdata from Salesforce to Analytics datalake
"""

import argparse
from pyprojects.utils.udfs import *
from simple_salesforce import Salesforce
import requests
import pandas as pd
from io import StringIO
from collections import OrderedDict


def run(args):

    id_pipeline = f"""{args.erp_type}_{args.btype}"""
    print(id_pipeline)

    i_url = get_creds(args.schema, id_pipeline, 'url')
    un = get_creds(args.schema, id_pipeline, 'username')
    pw = get_creds(args.schema, id_pipeline, 'password')
    oi = get_creds(args.schema, id_pipeline, 'organisation_id')
    ins = 'CS249'

    #flds = 'Name, Ageing_in_Days__c'
    flds = 'yr, cnt'
    tbl = 'Unit__c'
    incr = 'CreatedDate'
    incr_value = 365

    sf = Salesforce(instance_url=i_url, username=un, password=pw, organizationId=oi, domain='test', instance=ins)

    describe = sf.Opportunity.describe()
    for el in describe['fields']:
        print(el['name'])
    results = sf.query_all(f"""SELECT FIELDS(ALL) FROM {tbl} LIMIT 1""")
    #results = sf.query_all(f"""SELECT CALENDAR_YEAR(CreatedDate) yr, COUNT(Name) cnt FROM {tbl} GROUP BY CALENDAR_YEAR(CreatedDate)""")
    print(results['records'])
    r = pd.DataFrame.from_dict(results, orient='columns')
    print(r)
    d = []
    for r in results['records']:
        z = {}
        for k, v in r.items():
            if k in flds:
                z[k] = v
        print(z)
        d.append(z)
    print(pd.DataFrame(d))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with erp')
    parser.add_argument('-connection_name', dest='conn', required=False,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=False,
                        help="dwh schema to write in")
    parser.add_argument('-erp_type', dest='erp_type', required=False,
                        help="type of erp information system")
    parser.add_argument('-business_type', dest='btype', required=False,
                        help="b2c or b2b")
    parser.add_argument('-date', dest='dt', required=False,
                        help="start date to get the data")
    run(parser.parse_args())
