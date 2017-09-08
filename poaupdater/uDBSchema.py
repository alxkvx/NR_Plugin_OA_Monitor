__rcs_id__ = """$Id$"""

__pychecker__ = "unusednames=__rcs_id__,dummy"
import uDBTypes
import uSysDB


class NoSuchTable(Exception):

    def __init__(self, name):
        Exception.__init__(self, "%s: no such table" % name)


class Table:
    _cache = {}

    @staticmethod
    def reset_cache():
        Table._cache.clear()

    """Represents table declaration. Table instances have following attributes:

1. name - name of table
2. columns - a list of columns
3. constraints a list of constraints of table
4. indexes a list of indexes of table 

One can get Table class instance several ways:

1. Construct new definition with constructor.
2. Get existing table definition from database schema using getTable method
3. Get table definition from string that is fragment of PDL, with #uPDLDBSchema module """

    def __init__(self, name, columns, constraints, indexes, referenced_fk = []):
        self.name = name
        self.columns = columns
        self.constraints = constraints
        if indexes is None:
            self.indexes = []
        else:
            self.indexes = indexes
        self.referenced_fk = referenced_fk

    def reinit(self, con):
        if (con, self.name) in Table._cache:
            del Table._cache[(con, self.name)]
        tab = getTable(self.name, con)
        self.__init__(tab.name, tab.columns, tab.constraints, tab.indexes, tab.referenced_fk)

    def dropConstraint(self, name, con):
        """ table.dropConstraint(constraint_name, con). Drops constraint named constraint_name on table."""
        impl.dropConstraint(self, name, con)
        self.reinit(con)

    def dropIndex(self, name, con):
        """ table.dropIndex(index_name, con). Drops index named index_name on table."""
        impl.dropIndex(self, name, con)
        self.reinit(con)

    def addIndex(self, index, con):
        """  table.addIndex(index_def, con) Adds index index_def to table."""
        impl.createIndex(self, index, con)
        self.reinit(con)

    def addConstraint(self, constr, con):
        """ table.addConstraint(constraint_def, con) Adds constraint constraint_def to table. constraint_def should be an instance of one of following classes:

* uDBSchema.PrimaryKey
* uDBSchema.UniqueKey
* uDBSchema.ForeignKey
* uDBSchema.Check """
        impl.addConstraint(self, constr, con)
        self.reinit(con)

    def getConstraint(self, name):
        """ table.getConstraint(name) : returns constraint definition or None if table does not have constraint with specified name"""
        for c in self.constraints:
            if c.name == name:
                return c
        return None

    def getColumn(self, name):
        """table.getColumn(name) : returns column definition or None if table does not have column with specified name"""
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def getIndex(self, name):
        """table.getIndex(name) : returns index definition or None if table does not have index with specified name"""
        for i in self.indexes:
            if i.name == name:
                return i
        return None

    def create(self, con):
        """table.create(con) creates a table. con is a uSysDB connection instance."""
        impl.createTable(self, con)
        for index in self.indexes:
            impl.createIndex(self, index, con)

    def clone_begin(self, newname, con):
        self.reinit(con)
        newtab = Table(newname, self.columns, [], [])
        newtab.create(con)
        newtab.cloned_from = self.name
        return newtab

    def clone_finalize(self, con):

        cloned_from = getTable(self.cloned_from, con)
        for constraint in cloned_from.referenced_fk:
            reftab = getTable(constraint.reftable, con)
            reftab.dropConstraint(constraint.name, con)
        cloned_from.drop(con)

        self.rename(cloned_from.name, con)

        for index in cloned_from.indexes:
            newtab.addIndex(index, con)
        for constraint in cloned_from.constraints:
            newtab.addConstraint(constraint, con)
        for constraint in cloned_from.referenced_fk:
            reftab = getTable(constraint.reftable, con)
            reftab.addConstraint(ForeignKey(constraint.name, cloned_from.name, (constraint.columns[1], constraint.columns[0]), constraint.ondelete, constraint.onupdate), con)

    def drop(self, con):
        """table.drop(con) drops a table. con is a uSysDB connection instance."""
        impl.dropTable(self, con)
        if (con, self.name) in Table._cache:
            del Table._cache[(con, self.name)]

    def changeColumnType(self, column, new_type, con):
        impl.changeColumnType(self, column, new_type, con)
        self.reinit(con)

    def setNullable(self, column, con):
        impl.changeNullable(self, column, False, con)
        self.reinit(con)

    def dropNullable(self, column, con):
        impl.changeNullable(self, column, True, con)
        self.reinit(con)

    def setDefault(self, column, default, con):
        impl.setDefault(self, column, default, con)
        self.reinit(con)

    def dropColumn(self, name, con):
        impl.dropColumn(self, name, con)
        self.reinit(con)

    def addColumn(self, column, con):
        if not column.nullable and column.default is None and not isinstance(column.type, uDBTypes.SerialType):
            raise Exception("Cannot add not nullable column %s without default to table %s" % (column.name, self.name))
        impl.addColumn(self, column, con)
        self.reinit(con)

    def rename(self, new_name, con):
        impl.renameTable(self, new_name, con)
        self.name = new_name
        self.reinit(con)

    def renameColumn(self, name, new_name, con):
        impl.renameColumn(self, name, new_name, con)
        self.reinit(con)

    def identityInsertBegin(self, con):
        impl.identityInsertBegin(self, con)

    def identityInsertEnd(self, con):
        impl.identityInsertEnd(self, con)

    def dropIdentity(self, con):
        impl.dropIdentity(self, con)
        self.reinit(con)


class Column:

    """represent a table column declaration. Has following attributes:

* name - name of column
* type - type of column
* nullable - boolean that specifies whenever column is nullable
* default - default value for column. One of:
      o None - means that default value is NULL
      o expression (as str)- means that default value will be value of the expression
      o an instance of uDBTypes.DefSysDate class - means that default value will the time when record is modified ('DEFAULT now()') 

Column instances could be created by specifing it's attributesuDBSchema.Column(name, type, nullable=True, default=None), or column definition could be taken from existing Table instance. """

    def __init__(self, name, type, nullable=True, default=None, num=None):
        self.name = name
        self.type = type
        self.nullable = nullable
        self.default = default
        self.num = num

    def descr(self, table):
        return impl.columnDescription(self, table)


class Key:

    def __init__(self, name, columns):
        self.name = name
        if type(columns) not in (tuple, list):
            raise ValueError("Columns should be list or tuple, not %s" % type(columns).__name__)
        self.columns = columns

    def kind(self):
        return "Unknown key!"

    def descr(self, dummy=None):
        return impl.keyDescription(self)

    def short_descr(self):
        return "%s(%s)" % (self.name, ', '.join(self.columns))

    def refers(self, column_name):
        return column_name in self.columns


class PrimaryKey(Key):

    def __init__(self, name, columns):
        Key.__init__(self, name, columns)

    def getStatement(self, table):
        return "ALTER TABLE `%s` ADD CONSTRAINT `%s` PRIMARY KEY (%s)" % (table, self.name, impl.quotjoin(self.columns))

    def kind(self):
        return "PRIMARY KEY"


class UniqueKey(Key):

    def __init__(self, name, columns):
        Key.__init__(self, name, columns)

    def getStatement(self, table):
        return "ALTER TABLE `%s` ADD CONSTRAINT `%s` UNIQUE (%s)" % (table, self.name, impl.quotjoin(self.columns))

    def kind(self):
        return "UNIQUE"


def exprsEq(_1, _2):
    return impl.exprEq(_1, _2)


class Check:

    def __init__(self, name, expression):
        self.name = name
        if expression is None:
            raise Exception("expression can't be none")
        self.expression = expression

    def descr(self, dummy=None):
        return "CONSTRAINT `%s` CHECK (%s)" % (self.name, self.expression)

    def getStatement(self, table):
        return "ALTER TABLE `%s` ADD CONSTRAINT `%s` CHECK (%s)" % (table, self.name, self.expression)

    def refers(self, column_name):
        return self.expression.find(column_name) >= 0

    def kind(self):
        return "CHECK"


class Index(Key):

    def __init__(self, name, columns, unique=False):
        Key.__init__(self, name, columns)
        self.unique=unique

    def descr(self, table):
        UNIQUE = ''
        if self.unique:
            UNIQUE='UNIQUE'
        return "%s INDEX `%s` ON `%s`(%s)" % (UNIQUE, self.name, table.name, impl.quotjoin(self.columns))

    def kind(self):
        return "INDEX"


class ForeignKey:

    def __init__(self, name, reftable, columns, ondelete="NO ACTION", onupdate="NO ACTION"):
        self.name = name
        self.reftable = reftable
        if type(columns) == tuple:
            self.columns = columns
        else:
            self.columns = (columns, columns)
        self.ondelete = ondelete
        self.onupdate = onupdate

    def descr(self, dummy=None):
        return impl.fkDescription(self)

    def refers(self, column_name):
        return column_name in self.columns[0]

    def kind(self):
        return "FOREIGN KEY"

    def __str__(self):
        return self.name

    def __repr__(self):
        return "%s %s" % (self.name, self.ondelete)

    def generateIndex(self):
        return Index(self.name + "_idx", self.columns[0])

    def createIndex(self, table, con):
        return table.addIndex(self.generateIndex(), con)

    def getStatement(self, table):
        mycolumns, foreigncolumns = self.columns
        return "ALTER TABLE `%s` ADD CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s) ON DELETE %s ON UPDATE %s" % (table, self.name, impl.quotjoin(mycolumns), self.reftable, impl.quotjoin(foreigncolumns), self.ondelete, self.onupdate)

impl = None


def init(db_type):
    if uSysDB.DBType == uSysDB.PgSQL:
        import uDBSchemaPgSQL as implmodule
    else:
        import uDBSchemaMSSQL as implmodule

    global impl
    impl = implmodule


def getTable(name, con):
    if not impl:
        raise Exception("uDBSchema was not properly initialized. init() was not called?")

    if (con, name) not in Table._cache:
        Table._cache[(con, name)] = impl._getTable(name, con)
    return Table._cache[(con, name)]
#    return impl.getTable(name, con)


def getTableList(con):
    if not impl:
        raise Exception("uDBSchema was not properly initialized. init() was not called?")

    return impl.getTableList(con)


__all__ = ['Table', 'Index', 'ForeignKey', 'Key', 'PrimaryKey',
           'UniqueKey', 'NoSuchTable', 'Column', 'Check', 'getTable', 'init']
