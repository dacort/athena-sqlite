# Athena SQLite Driver

Using Athena's new [Query Federation](https://github.com/awslabs/aws-athena-query-federation/) functionality, read SQLite databases from S3.

Install it from the Serverless Application Repository: [AthenaSQLiteConnector](https://serverlessrepo.aws.amazon.com/#/applications/arn:aws:serverlessrepo:us-east-1:689449560910:applications~AthenaSQLITEConnector).

## Why?

I occasionally like to put together fun side projects over Thanksgiving and Christmas holidays.

I'd always joked it would a crazy idea to be able to read SQLite using Athena, so...here we are!

## How?

- I decided to use Python as I'm most familiar with it and because of the next point
- Using [APSW](https://rogerbinns.github.io/apsw/), we can implement a [Virtual File System](https://rogerbinns.github.io/apsw/vfs.html) (VFS) for S3
- Using the [Athena query federation example](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-example/), we can see what calls need to be implemented

The PyArrow library unfortunately weighs in over 250MB, so we have to use a custom compilation step to build a Lambda Layer.

## What?

Drop SQLite databases in a single prefix in S3, and Athena will list each file as a database and automatically detect tables and schemas.

Currently, all data types are strings. I'll fix this eventually.  All good things in time.

## Status

This project is under active development and very much in it's infancy.

Many things are hard-coded or broken into various pieces as I experiment and figure out how everything works.

## Building

The documentation for this is a work in progress. It's currently in between me creating the resources manually and building the assets for the AWS SAR,
and most of the docs will be automated away.

### Requirements

- Docker
- Python 3.7

### Lambda layer

First you need to build Lambda layer. There are two Dockerfiles and build scripts in the `lambda-layer/` directory.

We'll execute each of the build scripts and copy the results to the target directory. This is referenced by the SAR template, [`athena-sqlite.yaml`](athena-sqlite.yaml).

```
cd lambda-layer
./build.sh
./build-pyarrow.sh
cp -R layer/ ../target/
```

### Upload sample data

For the purpose of this test, we just have a sample sqlite database you can upload.

`aws s3 cp sample-data/sample_data.sqlite s3://<TARGET_BUCKET>/<TARGET_PREFIX>/`

Feel free to upload your own SQLite databases as well!

### Lambda function

There are three components to the Lambda code:

- `vfs.py` - A SQLite Virtual File System implementation for S3
- `s3qlite.py` - The actual Lambda function that handles Athena metadata/data requests
- `sqlite_db.py` - Helper functions for access SQLite databases on S3

Create a function with the code in [lambda-function/s3qlite.py](lambda-function/s3qlite.py) that uses the previously created layer.
The handler will be `s3qlite.lambda_handler`
Also include the `vfs.py` and `sqlite_db.py` files in your Lambda function

Configure two environment variables for your lambda function:
- `TARGET_BUCKET` - The name of your S3 bucket where SQLite files live
- `TARGET_PREFIX` - The prefix (e.g. `data/sqlite`) that you uploaded the sample sqlite database to

Note that the IAM role you associate the function with will also need `s3:GetObject` and `s3:ListBucket` access to wherever your lovely SQLite databases are stored.

### Configure Athena

Follow the Athena documentation for [Connecting to a data source](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source.html).
The primary thing to note here is that you need to create a workgroup named `AmazonAthenaPreviewFunctionality` and use that for your testing.
Some functionality will work in the primary workgroup, but you'll get weird errors when you try to query data.

I named my function `s3qlite` :)

### Run queries!

Here's a couple basic queries that should work:

```sql
SELECT * FROM "s3qlite"."sample_data"."records" limit 10;

SELECT COUNT(*) FROM "s3qlite"."sample_data"."records";
```

If you deploy the SAR app, the data catalog isn't registered automatically, but you can still run queries by using the special `lambda:` schema:

```sql
SELECT * FROM "lambda:s3qlite".sample_data.records LIMIT 10;
```

Where `s3qlite` is the value you provided for the `AthenaCatalogName` parameter.

## TODO

- Move these into issues :)
- Move vfs.py into it's own module
    - Maybe add write support to it someday :scream:
- Publish to SAR
- Add tests...always tests
- struct types, probably
- Don't read the entire file every time :)
- Escape column names with invalid characters
- Implement recursive listing

## Serverless App Repo

These are mostly notes I made while figuring out how to get SAR working.

Need to grant SAR access to the bucket

```shell
aws s3api put-bucket-policy --bucket <BUCKET> --region us-east-1 --policy '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service":  "serverlessrepo.amazonaws.com"
      },
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::<BUCKET>/*"
    }
  ]
}'
```

For publishing to the SAR, we just execute two commands

```shell
sam package --template-file athena-sqlite.yaml --s3-bucket <BUCKET> --output-template-file target/out.yaml
sam publish --template target/out.yaml --region us-east-1
```

If you want to deploy using CloudFormation, use this command:

```shell
sam deploy --template-file ./target/out.yaml --stack-name athena-sqlite --capabilities CAPABILITY_IAM --parameter-overrides 'DataBucket=<BUCKET> DataPrefix=tmp/sqlite' --region us-east-1
```
