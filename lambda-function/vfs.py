import apsw
import sys
import boto3

VFS_S3_CLIENT = boto3.client('s3')


class S3VFS(apsw.VFS):
    def __init__(self, vfsname="s3", basevfs=""):
        self.vfsname=vfsname
        self.basevfs=basevfs
        apsw.VFS.__init__(self, self.vfsname, self.basevfs)

    def xOpen(self, name, flags):
        return S3VFSFile(self.basevfs, name, flags)

class S3VFSFile():
    def __init__(self, inheritfromvfsname, filename, flags):
        self.bucket = filename.uri_parameter("bucket")
        self.key = filename.filename().lstrip("/")
        print("Initiated S3 VFS for file: {}".format(self._get_s3_url()))

    def xRead(self, amount, offset):
        response = VFS_S3_CLIENT.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(offset, offset + amount))
        response_data = response['Body'].read()
        return response_data
    
    def xFileSize(self):
        client = boto3.client('s3')
        response = client.head_object( Bucket=self.bucket, Key=self.key)
        return response['ContentLength']
    
    def xClose(self):
        pass

    def xFileControl(self, op, ptr):
        return False

    def _get_s3_url(self):
        return "s3://{}/{}".format(self.bucket, self.key)