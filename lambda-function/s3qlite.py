import pyarrow as pa
import apsw
import boto3
from vfs import S3VFS, S3VFSFile
from sqlite_db import SQLiteDB

import os
from uuid import uuid4
import base64

S3_BUCKET = os.environ['TARGET_BUCKET']
S3_PREFIX = os.environ['TARGET_PREFIX'].rstrip('/')  # Ensure that the prefix does *not* have a slash at the end

S3_CLIENT = boto3.client('s3')
S3FS = S3VFS()

# https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/src/main/java/com/amazonaws/athena/connector/lambda/handlers/FederationCapabilities.java#L33
CAPABILITIES = 23


class ListSchemasRequest:
    """List sqlite files in the defined prefix, do not recurse"""
    def execute(self, event):
        return {
            "@type": "ListSchemasResponse",
            "catalogName": event['catalogName'],
            "schemas": self._list_sqlite_objects(),
            "requestType": "LIST_SCHEMAS"
        }
    
    def _list_sqlite_objects(self):
        # We don't yet support recursive listing - everything must be in the prefix
        params = {
            'Bucket': S3_BUCKET,
            'Prefix': S3_PREFIX + '/',
            'Delimiter': '/'
        }
        sqlite_filenames = []
        while True:
            response = S3_CLIENT.list_objects_v2(**params)
            for data in response.get('Contents', []):
                sqlite_basename = data['Key'].replace(S3_PREFIX + '/', '').replace('.sqlite', '')
                sqlite_filenames.append(sqlite_basename)
            if 'NextContinuationToken' in response:
                params['ContinuationToken'] = response['NextContinuationToken']
            else:
                break
        return sqlite_filenames


class ListTablesRequest:
    """Given a sqlite schema (filename), return the tables of the database"""
    def execute(self, event):
        sqlite_dbname = event.get('schemaName')

        return {
            "@type": "ListTablesResponse",
            "catalogName": event['catalogName'],
            "tables": self._fetch_table_list(sqlite_dbname),
            "requestType": "LIST_TABLES"
        }
    
    def _fetch_table_list(self, sqlite_dbname):
        tables = []
        s3db = SQLiteDB(S3_BUCKET, S3_PREFIX, sqlite_dbname)
        for row in s3db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"):
            print("Found table: ", row[0])
            tables.append({'schemaName': sqlite_dbname, 'tableName': row[0]})
        return tables


class GetTableRequest:
    """Given a SQLite schema (filename) and table, return the schema"""
    def execute(self, event):
        databaseName = event['tableName']['schemaName']
        tableName = event['tableName']['tableName']
        columns = self._fetch_schema_for_table(databaseName, tableName)
        schema = self._build_pyarrow_schema(columns)
        print({
            "@type": "GetTableResponse",
            "catalogName": event['catalogName'],
            "tableName": {'schemaName': databaseName, 'tableName': tableName},
            "schema": {"schema": base64.b64encode(schema.serialize().slice(4)).decode("utf-8")},
            "partitionColumns": [],
            "requestType": "GET_TABLE"
        })
        return {
            "@type": "GetTableResponse",
            "catalogName": event['catalogName'],
            "tableName": {'schemaName': databaseName, 'tableName': tableName},
            "schema": {"schema": base64.b64encode(schema.serialize().slice(4)).decode("utf-8")},
            "partitionColumns": [],
            "requestType": "GET_TABLE"
        }
    
    def _fetch_schema_for_table(self, databaseName, tableName):
        columns = []
        s3db = SQLiteDB(S3_BUCKET, S3_PREFIX, databaseName)
        for row in s3db.execute("SELECT cid, name, type FROM pragma_table_info('{}')".format(tableName)):
            columns.append([row[1], row[2]])
        
        return columns
    
    def _build_pyarrow_schema(self, columns):
        """Return a pyarrow schema based on the SQLite data types, but for now ... everything is a string :)"""
        return pa.schema(
            [(col[0], pa.string()) for col in columns]
        )


class ReadRecordsRequest:
    def execute(self, event):
        schema = self._parse_schema(event['schema']['schema'])
        records = {k: [] for k in schema.names}
        sqlite_dbname = event['tableName']['schemaName']
        sqlite_tablename = event['tableName']['tableName']
        s3db = SQLiteDB(S3_BUCKET, S3_PREFIX, sqlite_dbname)

        # TODO: How to select field names?
        for row in s3db.execute("SELECT {} FROM {}".format(','.join(schema.names), sqlite_tablename)):
            for i, name in enumerate(schema.names):
                records[name].append(str(row[i]))

        pa_records = pa.RecordBatch.from_arrays([pa.array(records[name]) for name in schema.names], schema=schema)
        return {
            "@type": "ReadRecordsResponse",
            "catalogName": event['catalogName'],
            "records": {
                "aId": str(uuid4()),
                "schema": base64.b64encode(schema.serialize().slice(4)).decode("utf-8"),
                "records": base64.b64encode(pa_records.serialize().slice(4)).decode("utf-8")
            },
            "requestType": "READ_RECORDS"
        }

    def _parse_schema(self, encoded_schema):
        return pa.read_schema(pa.BufferReader(base64.b64decode(encoded_schema)))

class PingRequest:
    """Simple ping request that just returns some metadata"""
    def execute(self, event):
        return {
                    "@type": "PingResponse",
                    "catalogName": event['catalogName'],
                    "queryId": event['queryId'],
                    "sourceType": "sqlite",
                    "capabilities": CAPABILITIES
                }


def lambda_handler(event, context):
    print(event)
    # request_type = event['requestType']
    request_type = event['@type']
    if request_type == 'ListSchemasRequest':
        return ListSchemasRequest().execute(event)
    elif request_type == 'ListTablesRequest':
        return ListTablesRequest().execute(event)
    elif request_type == 'GetTableRequest':
        return GetTableRequest().execute(event)
    elif request_type == 'PingRequest':
        return PingRequest().execute(event)
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
        return ReadRecordsRequest().execute(event)
