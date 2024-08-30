import argparse
import pysftp
from zipfile import ZipFile
from pyprojects.utils.udfs import *


def run(args):

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    sftp = pysftp.Connection(host=get_creds('marketing', 'sftp', 'host'),
                             username=get_creds('marketing', 'sftp', 'username'),
                             password=get_creds('marketing', 'sftp', 'password'),
                             cnopts=cnopts)

    sftp.chdir(get_creds('marketing', 'sftp', 'folder'))

    d = datetime.datetime.now() #- datetime.timedelta(days=1)
    d = d.strftime("%d%m%Y")
    zf = f"{get_creds('marketing', 'sftp', 'archive_prefix')}_{d}.zip"
    print(zf)
    zf = ZipFile(sftp.open(zf))

    sftp_files = get_creds('marketing', 'sftp', 'files').split(',')

    for f in sftp_files:
        print(f)
        df = (pd.read_csv(zf.open(f'{f}.csv'), sep='|'))
        write_to_gbq(args.conn, args.schema, f'sftp_{f}',
                     clean_pandas_dataframe(df, '', True, datetime.datetime.now()), 'append')
    sftp.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with eoi data')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    run(parser.parse_args())