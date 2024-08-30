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

    if zf in sftp.listdir():

        zf = ZipFile(sftp.open(zf))

        for zff in zf.filelist:
            print(zff.filename.split('.')[0])
            df = pd.read_csv(zf.open(zff.filename), sep='|')
            write_to_gbq(args.conn, args.schema, f"sftp_{zff.filename.split('.')[0]}",
                         clean_pandas_dataframe(df, '', True, datetime.datetime.now()), 'append')
    sftp.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with eoi data')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    run(parser.parse_args())