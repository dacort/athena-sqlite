# Athena SQLite Driver

Using Athena's new [Query Federation](https://github.com/awslabs/aws-athena-query-federation/) functionality, read SQLite databases from S3.

## Why?

I occasionally like to put together fun side projects over Thanksgiving and Christmas holidays.

I'd always joked it would a crazy idea to be able to read SQLite using Athena, so...here we are!

## How?

- I decided to use Python as I'm most familiar with it and because of the next point
- Using [APSW](https://rogerbinns.github.io/apsw/), we can implement a [Virtual File System](https://rogerbinns.github.io/apsw/vfs.html) (VFS) for S3
- Using the [Athena query federation example](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-example/), we can see what calls need to be implemented

The PyArrow library unfortunately weighs in over 250MB, so we have to use a custom compilation step to build a Lambda Layer.

## What?

The goal is to be able to have this driver list sqlite databases in a configurable S3 bucket and prefix. 
Each database file would show up as a separate database in the Athena GUI with the subset of tables per database.

Currently, we just use a slightly hardcoded sample database. All good things in time.

## Status

This project is under active development and very much in it's infancy.

Many things are hard-coded or broken into various pieces as I experiment and figure out how everything works.

## Building

### Requirements

- Docker
- Python 3.7

### Lambda layer

First you need to build and upload the Lambda layer. There are two Dockerfiles and build scripts in the `lambda-layer/` directory.

```
cd lambda-layer
./build.sh
./build-pyarrow.sh
cd layer
zip -r python_libs.zip python
aws s3 cp python_libs.zip s3://<BUCKET>/lambda/layers/apsw/
```

Once you've done that, create a new Lambda layer that points to `https://<BUCKET>.s3.amazonaws.com/lambda/layers/apsw/python_libs.zip`.
Choose `Python 3.7` as a compatible runtime.

### Upload sample data

For the purpose of this test, we just have a sample sqlite database you can upload.

`aws s3 cp sample-data/sample_data.sqlite s3://<TARGET_BUCKET>/<TARGET_PREFIX>/`

### Lambda function

There are two components to the Lambda code:

- `vfs.py` - A SQLite Virtual File System implementation for S3
- `s3qlite.py` - The actual Lambda function that handles Athena metadata/data requests

Create a function with the code in [lambda-function/s3qlite.py](lambda-function/s3qlite.py) that uses the previously created layer.
The handler will be `s3qlite.lambda_handler`
Also include the `vfs.py` file in your Lambda function

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