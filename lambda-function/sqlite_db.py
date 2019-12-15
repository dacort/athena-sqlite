import apsw
from vfs import S3VFS, S3VFSFile

S3FS = S3VFS()


class SQLiteDB:
    def __init__(self, bucket, prefix, database_name):
        self.bucket = bucket
        self.prefix = prefix
        self.database_name = database_name

        self.connection = self._build_connection()
        self.cursor = self.connection.cursor()
    
    def execute(self, query):
        return self.cursor.execute(query)
    
    def _build_connection(self):
        return apsw.Connection(self._build_sqlite_s3_uri(),
                       flags=apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI,
                       vfs=S3FS.vfsname)

    def _build_sqlite_s3_uri(self):
        """Build a SQLite-compatible URI
        
        If this wasn't a `file:` identifier, SQLite would throw an error.
        We also include the `immutable` flag or SQLite will try to create a `-journal` file and fail.
        """
        return "file:/{}/{}.sqlite?bucket={}&immutable=1".format(self.prefix, self.database_name, self.bucket)