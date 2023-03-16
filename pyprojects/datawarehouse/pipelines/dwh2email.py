"""
Script to share analytics data via email
"""

import argparse
from pyprojects.utils.udfs import *


def run(args):

    text = "WIP text of the message"
    send_email(email='', text=text)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with spryker')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    run(parser.parse_args())