import apsw
import sys
import boto3
py3=sys.version_info >= (3, 0)

def encryptme(data):
    if not data: return data
    if py3:
        return bytes([x^0xa5 for x in data])
    return "".join([chr(ord(x)^0xa5) for x in data])

# Inheriting from a base of "" means the default vfs
class ObfuscatedVFS(apsw.VFS):
    def __init__(self, vfsname="obfu", basevfs=""):
        self.vfsname=vfsname
        self.basevfs=basevfs
        apsw.VFS.__init__(self, self.vfsname, self.basevfs)

    # We want to return our own file implmentation, but also
    # want it to inherit
    def xOpen(self, name, flags):
        # We can look at uri parameters
        if isinstance(name, apsw.URIFilename):
            #@@CAPTURE
            print ("fast is", name.uri_parameter("fast"))
            print ("level is", name.uri_int("level", 3))
            print ("warp is", name.uri_boolean("warp", False))
            print ("notpresent is", name.uri_parameter("notpresent"))
            #@@ENDCAPTURE
        return ObfuscatedVFSFile(self.basevfs, name, flags)

# The file implementation where we override xRead and xWrite to call our
# encryption routine
class ObfuscatedVFSFile(apsw.VFSFile):
    def __init__(self, inheritfromvfsname, filename, flags):
        apsw.VFSFile.__init__(self, inheritfromvfsname, filename, flags)

    def xRead(self, amount, offset):
        return encryptme(super(ObfuscatedVFSFile, self).xRead(amount, offset))

    def xWrite(self, data, offset):
        super(ObfuscatedVFSFile, self).xWrite(encryptme(data), offset)


class S3VFS(apsw.VFS):
    def __init__(self, vfsname="s3", basevfs=""):
        self.vfsname=vfsname
        self.basevfs=basevfs
        apsw.VFS.__init__(self, self.vfsname, self.basevfs)

    # We want to return our own file implmentation, but also
    # want it to inherit
    def xOpen(self, name, flags):
        return S3VFSFile(self.basevfs, name, flags)

class S3VFSFile():
    def __init__(self, inheritfromvfsname, filename, flags):
        print("HELLO", filename.filename(), flags)
        print ("bucket is", filename.uri_parameter("bucket"))
        self.bucket = filename.uri_parameter("bucket")
        self.key = filename.filename().lstrip("/")
        # apsw.VFSFile.__init__(self, inheritfromvfsname, filename, flags)

    def xRead(self, amount, offset):
        # return encryptme(super(ObfuscatedVFSFile, self).xRead(amount, offset))
        print("xRead from s3://" + self.bucket + "/" + self.key + ". ", amount, " bytes starting from ", offset)
        client = boto3.client('s3')
        response = client.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(offset, offset + amount))
        response_data = response['Body'].read()
        return response_data
    
    def xFileSize(self):
        client = boto3.client('s3')
        response = client.head_object( Bucket=self.bucket, Key=self.key)
        return response['ContentLength']

    def xWrite(self, data, offset):
        raise NotImplementedError

    def xClose(self):
        pass

    def xUnlock(self, level):
        pass

    def xLock(self, level):
        pass

    def xFileControl(self, op, ptr):
        return True