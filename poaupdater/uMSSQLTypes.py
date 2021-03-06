IMAGE = 0x22
TEXT = 0x23
NTEXT = 0x63
TINYINT = 0x30
SMALLINT = 0x34
INT = 0x38
BIGINT = 0x7F
BIT = 0x68
CHAR = 0xAF
VARCHAR = 0xA7
NVARCHAR = 0xE7
NCHAR = 0xEF
BINARY = 0xAD
VARBINARY = 0xA5
REAL = 0x3B
FLOAT = 0x3E
DECIMAL = 0x6A
NUMERIC = 0x6C
MONEY = 0x3C
SMALLMONEY = 0x7A
SMALLDATETIME = 0x3A
DATETIME = 0x3D
UNIQUEIDENTIFIER = 0x24
SQL_VARIANT = 0x62
TIMESTAMP = 0xBD

char_types = [CHAR, NCHAR]

blob_types = [IMAGE, TEXT, NTEXT, BINARY]

varchar_types = [VARCHAR, NVARCHAR]

timestamp_types = [DATETIME]

int_types = [INT, SMALLINT]

bigint_types = [BIGINT]
