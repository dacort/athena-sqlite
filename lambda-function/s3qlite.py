import pyarrow as pa
import apsw
from vfs import S3VFS, S3VFSFile

import os
from uuid import uuid4
import base64

S3_BUCKET = os.environ['TARGET_BUCKET']
S3_PREFIX = os.environ['TARGET_PREFIX']

# https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/src/main/java/com/amazonaws/athena/connector/lambda/handlers/FederationCapabilities.java#L33
CAPABILITIES = 23

def lambda_handler(event, context):
    print(event)
    # request_type = event['requestType']
    request_type = event['@type']
    if request_type == 'ListSchemasRequest':
        return {
            "@type": "ListSchemasResponse",
            "catalogName": event['catalogName'],
            "schemas": [
                "sample_data"
            ],
            "requestType": "LIST_SCHEMAS"
        }
    elif request_type == 'ListTablesRequest':
        tables = []
        s3fs=S3VFS()
        # s3db=apsw.Connection("file:sample_datadata.sqlite", vfs=s3fs.vfsname)
        s3db=apsw.Connection("file:/{}/sample_data.sqlite?bucket={}".format(S3_PREFIX, S3_BUCKET),
                       flags=apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI,
                       vfs=s3fs.vfsname)
        cursor=s3db.cursor()
        for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"):
            print("Found table: ", row[0])
            tables.append({'schemaName': event['schemaName'], 'tableName': row[0]})
        return {
            "@type": "ListTablesResponse",
            "catalogName": event['catalogName'],
            "tables": tables,
            "requestType": "LIST_TABLES"
        }
    elif request_type == 'GetTableRequest':
        databaseName = event['tableName']['schemaName']
        tableName = event['tableName']['tableName']
        # s3fs=S3VFS()
        # s3db=apsw.Connection("file:sample_datadata.sqlite", vfs=s3fs.vfsname)
        # cursor=s3db.cursor()
        print("passing back faux table")
        s = pa.schema([('year', pa.int64()), ('month', pa.int64()), ('day', pa.int64()), ('someval', pa.string())])
        print({
            "@type": "GetTableResponse",
            "catalogName": event['catalogName'],
            "tableName": {'schemaName': databaseName, 'tableName': tableName},
            "schema": {"schema": base64.b64encode(s.serialize().slice(4)).decode("utf-8")},
            "partitionColumns": [],
            "requestType": "GET_TABLE"
        })
        return {
            "@type": "GetTableResponse",
            "catalogName": event['catalogName'],
            "tableName": {'schemaName': databaseName, 'tableName': tableName},
            "schema": {"schema": base64.b64encode(s.serialize().slice(4)).decode("utf-8")},
            "partitionColumns": [],
            "requestType": "GET_TABLE"
        }
    elif request_type == 'PingRequest':
        return {
            "@type": "PingResponse",
            "catalogName": event['catalogName'],
            "queryId": event['queryId'],
            "sourceType": "sqlite",
            "capabilities": CAPABILITIES
        }
    elif request_type == 'GetTableLayoutRequest':
        databaseName = event['tableName']['schemaName']
        tableName = event['tableName']['tableName']
        # If the data is partitioned, this sends back the partition schema
        # Block schema is defined in BlockSerializer in the Athena Federation SDK
        block = {
            'aId': str(uuid4()),
            'schema': base64.b64encode(pa.schema({}).serialize().slice(4)).decode("utf-8"),
            'records': base64.b64encode(pa.RecordBatch.from_arrays([]).serialize().slice(4)).decode("utf-8")
        }
        # Unsure how to do this with an "empty" block.
        # Used this response from the cloudwatch example and it worked:
        # >>> schema
        # partitionId: int32
        # metadata
        # --------
        # {}
        # 
        # >>> batch.columns
        # [<pyarrow.lib.Int32Array object at 0x7eff750fff30>
        # [
        #   1
        # ]]
        cloudwatch = {
            "aId": str(uuid4()),
            "schema": "nAAAABAAAAAAAAoADgAGAA0ACAAKAAAAAAADABAAAAAAAQoADAAAAAgABAAKAAAACAAAAAgAAAAAAAAAAQAAABgAAAAAABIAGAAUABMAEgAMAAAACAAEABIAAAAUAAAAFAAAABwAAAAAAAIBIAAAAAAAAAAAAAAACAAMAAgABwAIAAAAAAAAASAAAAALAAAAcGFydGl0aW9uSWQAAAAAAA==",
            "records": "jAAAABQAAAAAAAAADAAWAA4AFQAQAAQADAAAABAAAAAAAAAAAAADABAAAAAAAwoAGAAMAAgABAAKAAAAFAAAADgAAAABAAAAAAAAAAAAAAACAAAAAAAAAAAAAAABAAAAAAAAAAgAAAAAAAAABAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAABAAAAAAAAAA=="
        }
        # Let's use cloudwatch for now, that gets me to GetSplitsRequest
        return {
            "@type": "GetTableLayoutResponse",
            "catalogName": event['catalogName'],
            "tableName": {'schemaName': databaseName, 'tableName': tableName},
            "partitions": cloudwatch,
            "requestType": "GET_TABLE_LAYOUT"
        }
    elif request_type == 'GetSplitsRequest':
        # The splits don't matter to Athena, it's mostly hints to pass on to ReadRecordsRequest
        return {
            "@type": "GetSplitsResponse",
            "catalogName": event['catalogName'],
            "splits": [
                {
                    "spillLocation": {
                        "@type": "S3SpillLocation",
                        "bucket": S3_BUCKET,
                        "key": "athena-spill/7b2b96c9-1be5-4810-ac2a-163f754e132c/1a50edb8-c4c7-41d7-8a0d-1ce8e510755f",
                        "directory": True
                    },
                    "properties": {}
                }
            ],
            "continuationToken": None,
            "requestType": "GET_SPLITS"
        }
    elif request_type == 'ReadRecordsRequest':
        records = {
            'year': [],
            'month': [],
            'day': [],
            'someval': []
        }
        # NEed a response builder
        # May do a table first and convert to recordbatch?
        # https://stackoverflow.com/questions/57939092/fastest-way-to-construct-pyarrow-table-row-by-row
        s3fs=S3VFS()
        # s3db=apsw.Connection("file:sample_datadata.sqlite", vfs=s3fs.vfsname)
        s3db=apsw.Connection("file:/{}/sample_data.sqlite?bucket={}".format(S3_PREFIX, S3_BUCKET),
                       flags=apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI,
                       vfs=s3fs.vfsname)
        cursor=s3db.cursor()
        for row in cursor.execute("SELECT * FROM records"):
            records['year'].append(row[0])
            records['month'].append(row[1])
            records['day'].append(row[2])
            records['someval'].append(row[3])

        schema = pa.schema([('year', pa.int64()), ('month', pa.int64()), ('day', pa.int64()), ('someval', pa.string())])
        records = pa.RecordBatch.from_arrays([pa.array(records['year']), pa.array(records['month']), pa.array(records['day']), pa.array(records['someval'])], schema=schema)
        return {
            "@type": "ReadRecordsResponse",
            "catalogName": event['catalogName'],
            "records": {
                "aId": str(uuid4()),
                "schema": base64.b64encode(schema.serialize().slice(4)).decode("utf-8"),
                "records": base64.b64encode(records.serialize().slice(4)).decode("utf-8")
            },
            "requestType": "READ_RECORDS"
        }
