__rcs_id__ = """$Id$"""


class DBError:

    def __repr__(self):
        return "<%s>" % self


class DBErrorMissingTable(DBError):

    def __init__(self, _schema):
        self.schema = _schema
        self.name = "missing table"

    def __str__(self):
        return "%s '%s'" % (self.name, self.schema.name)


class DBErrorRedundantTable(DBError):

    def __init__(self, _table):
        self.table = _table
        self.name = "redundant table"

    def __str__(self):
        return "%s '%s'" % (self.name, self.table.name)


class DBErrorMissingColumn(DBError):

    def __init__(self, _table, _schema_column):
        self.table = _table
        self.schema_column = _schema_column
        self.name = "missing column"

    def __str__(self):
        return "%s '%s' in table '%s': \"%s\"" % (self.name, self.schema_column.name, self.table.name, self.schema_column.descr(self.table))


class DBErrorRedundantColumn(DBError):

    def __init__(self, _table, _table_column):
        self.table = _table
        self.table_column = _table_column
        self.name = "redundant column"

    def __str__(self):
        return "%s '%s' in table '%s': \"%s\"" % (self.name, self.table_column.name, self.table.name, self.table_column.descr(self.table))


class DBErrorNonconformingColumnType(DBError):

    def __init__(self, _table, _table_column, _schema_column):
        self.table = _table
        self.table_column = _table_column
        self.schema_column = _schema_column
        self.name = "nonconforming type of column"

    def __str__(self):
        return "%s '%s' in table '%s': \"%s\", must be \"%s\"" % (self.name, self.table_column.name, self.table.name, self.table_column.type, self.schema_column.type)


class DBErrorColumnMissingNotNull(DBError):

    def __init__(self, _table, _table_column):
        self.table = _table
        self.table_column = _table_column
        self.name = "missing not null constraint"

    def __str__(self):
        return "%s on column '%s' in table '%s'" % (self.name, self.table_column.name, self.table.name)


class DBErrorColumnRedundantNotNull(DBError):

    def __init__(self, _table, _table_column):
        self.table = _table
        self.table_column = _table_column
        self.name = "redundant not null constraint"

    def __str__(self):
        return "%s on column '%s' in table '%s'" % (self.name, self.table_column.name, self.table.name)


class DBErrorColumnMissingDefault(DBError):

    def __init__(self, _table, _table_column, _default):
        self.table = _table
        self.table_column = _table_column
        self.default = _default
        self.name = "missing default value"

    def __str__(self):
        return "%s for column '%s' in table '%s': must be \"%s\"" % (self.name, self.table_column.name, self.table.name, self.default)


class DBErrorColumnNonconformingDefault(DBError):

    def __init__(self, _table, _table_column, _default):
        self.table = _table
        self.table_column = _table_column
        self.default = _default
        self.name = "nonconforming default value"

    def __str__(self):
        return "%s for column '%s' in table '%s': \"%s\", must be \"%s\"" % (self.name, self.table_column.name, self.table.name, self.table_column.default, self.default)


class DBErrorColumnRedundantDefault(DBError):

    def __init__(self, _table, _table_column):
        self.table = _table
        self.table_column = _table_column
        self.name = "redundant default value"

    def __str__(self):
        return "%s for column '%s' in table '%s': \"%s\"" % (self.name, self.table_column.name, self.table.name, self.table_column.default)


class DBErrorMissingPrimaryKey(DBError):

    def __init__(self, _table, _schema_pk):
        self.table = _table
        self.schema_pk = _schema_pk
        self.name = "missing primary key constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.schema_pk.name, self.table.name, self.schema_pk.descr())


class DBErrorNonconformingPrimaryKey(DBError):

    def __init__(self, _table, _table_pk, _schema_pk):
        self.table = _table
        self.table_pk = _table_pk
        self.schema_pk = _schema_pk
        self.name = "nonconforming primary key constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\", must be \"%s\"" % (self.name, self.table_pk.name, self.table.name, self.table_pk.descr(), self.schema_pk.descr())


class DBErrorRedundantPrimaryKey(DBError):

    def __init__(self, _table, _table_pk):
        self.table = _table
        self.table_pk = _table_pk
        self.name = "redundant primary key constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.table_pk.name, self.table.name, self.table_pk.descr())


class DBErrorMissingForeignKey(DBError):

    def __init__(self, _table, _schema_fk):
        self.table = _table
        self.schema_fk = _schema_fk
        self.name = "missing foreign key constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.schema_fk.name, self.table.name, self.schema_fk.descr())


class DBErrorNonconformingForeignKey(DBError):

    def __init__(self, _table, _table_fk, _schema_fk):
        self.table = _table
        self.table_fk = _table_fk
        self.schema_fk = _schema_fk
        self.name = "nonconforming foreign key constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\", must be \"%s\"" % (self.name, self.table_fk.name, self.table.name, self.table_fk.descr(), self.schema_fk.descr())


class DBErrorRedundantForeignKey(DBError):

    def __init__(self, _table, _table_fk):
        self.table = _table
        self.table_fk = _table_fk
        self.name = "redundant foreign key constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.table_fk.name, self.table.name, self.table_fk.descr())


class DBErrorMissingUnique(DBError):

    def __init__(self, _table, _schema_unique):
        self.table = _table
        self.schema_unique = _schema_unique
        self.name = "missing unique constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.schema_unique.name, self.table.name, self.schema_unique.descr())


class DBErrorNonconformingUnique(DBError):

    def __init__(self, _table, _table_unique, _schema_unique):
        self.table = _table
        self.table_unique = _table_unique
        self.schema_unique = _schema_unique
        self.name = "nonconforming unique key constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\", must be \"%s\"" % (self.name, self.table_unique.name, self.table.name, self.table_unique.descr(), self.schema_unique.descr())


class DBErrorRedundantUnique(DBError):

    def __init__(self, _table, _table_unique):
        self.table = _table
        self.table_unique = _table_unique
        self.name = "redundant unique key constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.table_unique.name, self.table.name, self.table_unique.descr())


class DBErrorMissingCheck(DBError):

    def __init__(self, _table, _schema_check):
        self.table = _table
        self.schema_check = _schema_check
        self.name = "missing check constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.schema_check.name, self.table.name, self.schema_check.descr())


class DBErrorNonconformingCheck(DBError):

    def __init__(self, _table, _table_check, _schema_check):
        self.table = _table
        self.table_check = _table_check
        self.schema_check = _schema_check
        self.name = "nonconforming check constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\", must be \"%s\"" % (self.name, self.table_check.name, self.table.name, self.table_check.descr(), self.schema_check.descr())


class DBErrorRedundantCheck(DBError):

    def __init__(self, _table, _table_check):
        self.table = _table
        self.table_check = _table_check
        self.name = "redundant check constraint"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.table_check.name, self.table.name, self.table_check.descr())


class DBErrorMissingIndex(DBError):

    def __init__(self, _table, _schema_index):
        self.table = _table
        self.schema_index = _schema_index
        self.name = "missing index"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.schema_index.name, self.table.name, self.schema_index.descr(self.table))


class DBErrorNonconformingIndex(DBError):

    def __init__(self, _table, _table_index, _schema_index):
        self.table = _table
        self.table_index = _table_index
        self.schema_index = _schema_index
        self.name = "nonconforming index"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\", must be \"%s\"" % (self.name, self.table_index.name, self.table.name, self.table_index.descr(self.table), self.schema_index.descr(self.table))


class DBErrorRedundantIndex(DBError):

    def __init__(self, _table, _table_index):
        self.table = _table
        self.table_index = _table_index
        self.name = "redundant index"

    def __str__(self):
        return "%s '%s' on table '%s': \"%s\"" % (self.name, self.table_index.name, self.table.name, self.table_index.descr(self.table))


class DBErrorMissingSequence(DBError):  # not used yet
    pass


class DBErrorRedundantSequence(DBError):  # not used yet
    pass


class DBErrorRedundantView(DBError):  # not used yet
    pass


class DBErrorRedundantTrigger(DBError):  # not used yet
    pass


class DBErrorNonconformingFunction(DBError):  # not used yet
    pass


class DBErrorRedundantFunction(DBError):  # not used yet
    pass
