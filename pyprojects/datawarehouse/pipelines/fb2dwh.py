"""
Script to bring pgdata from Facebook Api to Analytics datalake
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
import psycopg2
import time
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
import re
import requests


def run(args):

    project = 'marketing'
    pipeline = f"""{args.schema}"""

    # Set the login info
    app_id = get_creds(project, pipeline, 'app_id')
    app_secret = get_creds(project, pipeline, 'app_secret')
    access_token = get_creds(project, pipeline, 'access_token')

    FacebookAdsApi.init(app_id, app_secret, access_token)

    accounts_list = get_creds(project, pipeline, 'accounts_list').split(',')

    for acnt in accounts_list:
        my_account = AdAccount(f'act_{str(acnt)}')
        campaigns = my_account.get_campaigns()

        campaigns_list = re.findall(r'\d+', str(campaigns))
        print(campaigns_list)

        params = {
            'fields': get_creds(project, pipeline, 'params_fields').split(',')
                    }
        df = pd.DataFrame()

        for cmpgn in campaigns_list:
            campaign_actions = pd.DataFrame()

            campaign = Campaign(int(cmpgn))
            response = campaign.get_insights(params=params)

            campaign_data = pd.json_normalize(json.loads(str(response[0]).replace('<AdsInsights> ','')))

            if 'actions' in campaign_data.columns:
                for a in campaign_data['actions']:

                    campaign_actions = pd.concat([campaign_actions, pd.DataFrame.from_dict(a)])

                campaign_data = campaign_data.drop(columns=['actions'])

                df = pd.concat([df, campaign_actions.merge(campaign_data, how='cross')])
            else:
                df = pd.concat([df, campaign_data])

        write_to_gbq(args.conn, args.schema, 'fb_ads_requests',
                     clean_pandas_dataframe(df, 'fb2dwh', '', datetime.datetime.now()), 'append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with eoi data')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    run(parser.parse_args())