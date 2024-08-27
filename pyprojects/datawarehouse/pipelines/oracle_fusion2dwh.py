import argparse
from pyprojects.utils.udfs import *
import oci
from ocifs import OCIFileSystem


def run(args):

    project = 'erp'
    pipeline = f"""{args.schema}"""


    config = oci.config.from_file("~/.oci/config", "DEFAULT")
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    fs = OCIFileSystem("~/.oci/config")

    ocp_namespace_name = get_creds('oracle', 'fusion', 'namespace_name')
    ocp_namespace_bucket_name = get_creds('oracle', 'fusion', 'bucket_name')
    if args.dwh_dataset == '':
        dst=args.file_infix
    else:
        dst = args.dwh_datset

    list_objects_response = object_storage_client.list_objects(namespace_name=ocp_namespace_name,
                                                               bucket_name=ocp_namespace_bucket_name)

    l = json.loads(str(list_objects_response.data))

    for el in l['objects']:
        if args.file_infix in el['name'] and '.zip' in el['name']:
            print(el['name'])
            fname = el['name']
            get_object_response = object_storage_client.get_object(
                namespace_name=ocp_namespace_name,
                bucket_name=ocp_namespace_bucket_name,
                object_name=el['name'])

    df = pd.read_csv(
        f"oci://{ocp_namespace_bucket_name}@{ocp_namespace_name}/{fname}",
        storage_options={"config": "~/.oci/config"}, low_memory=False)

    print(df)

    write_to_gbq(args.conn, args.schema, dst,
                 clean_pandas_dataframe(df, 'salesforce', True, datetime.datetime.now()), 'replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sourcing datawarehouse with erp')
    parser.add_argument('-connection_name', dest='conn', required=True,
                        help="connection name to gbq")
    parser.add_argument('-schema_name', dest='schema', required=True,
                        help="dwh schema to write in")
    parser.add_argument('-file_infix', dest='file_infix', required=True,
                        help="ocs file infix to locate the right file name")
    parser.add_argument('-dwh_dataset', dest='dwh_dataset', required=False,
                        help="datawarehouse dataset to write data in")
    run(parser.parse_args())
