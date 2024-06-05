import argparse, oracledb
from pyprojects.utils.udfs import *

def run(args):

    un = get_creds('oracle', 'paas', 'username')
    cs = get_creds('oracle', 'paas', 'servicename')
    pw = get_creds('oracle', 'paas', 'password')

    tbl = 'AGENT_DL'

    with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
        columns = []
        data = []
        with connection.cursor() as tbl_fields:
            sql = f"""SELECT COLUMN_NAME FROM all_tab_columns WHERE table_name='{tbl}'"""
            for r in tbl_fields.execute(sql):
                columns.append(r[0])
        print(columns)
        with connection.cursor() as tbl_data:
            sql = f"""select * from {tbl}"""
            for r in tbl_data.execute(sql):
                data.append(r)
    df = pd.DataFrame(data, columns=columns)
    print(df)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with erp')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    run(parser.parse_args())
