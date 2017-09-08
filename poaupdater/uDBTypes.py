__rcs_id__ = """$Id$"""


class Type:

    def __init__(self, *args):
        self.args = args
        pass

    def __str__(self):
        return "Unknown Type %s" % (self.args, )

    def MSSQL_name(self):
        return self.__str__()

    def PgSQL_name(self):
        return self.__str__()


class IntType(Type):

    def __init__(self):
        Type.__init__(self)

    def __str__(self):
        return "INT"


class SerialType(IntType):

    def __init__(self, startswith=1):
        IntType.__init__(self)
        self.startswith = startswith

    def __str__(self):
        return "SERIAL"

    def PgSQL_name(self):
        return "INT"

    def MSSQL_name(self):
        return "INT IDENTITY (%d, 1)" % self.startswith


class BigIntType(Type):

    def __init__(self):
        Type.__init__(self)

    def __str__(self):
        return "BIGINT"


class CharType(Type):

    def __init__(self, size):
        Type.__init__(self)
        self.size = size

    def __str__(self):
        return "CHAR (%d)" % self.size

    def MSSQL_name(self):
        return "NCHAR (%d) COLLATE Latin1_General_CS_AS " % self.size


class VarcharType(Type):

    def __init__(self, size):
        Type.__init__(self)
        self.size = size

    def __str__(self):
        return "VARCHAR (%d)" % self.size

    def MSSQL_name(self):
        return "NVARCHAR (%d) COLLATE Latin1_General_CS_AS " % self.size


class BlobType(Type):

    def __init__(self):
        Type.__init__(self)

    def __str__(self):
        return "BLOB"

    def PgSQL_name(self):
        return "BYTEA"

    def MSSQL_name(self):
        return "IMAGE"


class DoubleType(Type):

    def __init__(self):
        Type.__init__(self)

    def __str__(self):
        return "DOUBLE"

    def PgSQL_name(self):
        return "DOUBLE PRECISION"

    def MSSQL_name(self):
        return "float(53)"


class TimestampType(Type):

    def __init__(self):
        Type.__init__(self)

    def __str__(self):
        return "TIMESTAMP"

    def MSSQL_name(self):
        return "DATETIME"

    def PgSQL_name(self):
        return "TIMESTAMPTZ"


class NumericType(Type):

    def __init__(self, precision, scale):
        Type.__init__(self)
        self.precision = precision
        self.scale = scale

    def __str__(self):
        return "NUMERIC (%d, %d)" % (self.precision, self.scale)


class DefSysDate:

    def __init__(self):
        pass


__all__ = ["CharType", "VarcharType", "BlobType", "IntType", "Type"]
