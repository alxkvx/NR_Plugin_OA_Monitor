__rcs_id__ = """$Id$"""
from uDBSchema import *
import uDBTypes
import uMSSQLTypes
from uGenerics import first
import uLogging

# uDBSchemaMSSQL  module should never be imported directly. it is
# implementation of MSSQL specific part of uDBSchema module


def quotjoin(a):
    return ', '.join(a)


def getColumnType(typ, sz):
    # character types are stored in 2-byte encoding. so sz/2
    if typ in uMSSQLTypes.char_types:
        return uDBTypes.CharType(sz / 2)
    elif typ in uMSSQLTypes.varchar_types:
        return uDBTypes.VarcharType(sz / 2)
    elif typ in uMSSQLTypes.blob_types:
        return uDBTypes.BlobType()
    elif typ in uMSSQLTypes.int_types:
        return uDBTypes.IntType()
    elif typ in uMSSQLTypes.timestamp_types:
        return uDBTypes.TimestampType()
    elif typ == uMSSQLTypes.UNIQUEIDENTIFIER:
        return uDBTypes.SerialType()
    elif typ in uMSSQLTypes.bigint_types:
        return uDBTypes.BigIntType()

    else:
        return uDBTypes.Type(typ, sz)


act_mapping = {
    0: 'NO ACTION',
    1: 'CASCADE',
    2: 'SET NULL'
}


def _getTableOid(name, cur):
    cur.execute("SELECT object_id FROM sys.objects WHERE name = %s", name)
    row = cur.fetchone()
    if not row:
        raise NoSuchTable(name)

    return row[0]


def _getTable(name, con):
    indexes = []

    cur = con.cursor()
    try:
        oid = _getTableOid(name, cur)

        # Columns
        cur.execute(
            "SELECT col.name, col.system_type_id, col.max_length, col.is_nullable, CAST (def.definition AS VARCHAR), col.column_id, col.is_identity FROM sys.columns col LEFT JOIN sys.default_constraints def ON (def.object_id = col.default_object_id) WHERE col.object_id = %s ORDER by col.column_id", oid)
        columns = []

        for row in cur.fetchall():
            if not row[4]:
                default = None
            elif row[4] == '(getdate())':
                default = uDBTypes.DefSysDate()
            else:
                default = row[4].strip('()')

            is_identity = int(row[6])
            is_nullable = int(row[3])
            if is_identity:
                coltype = uDBTypes.SerialType()
            else:
                coltype = getColumnType(row[1], row[2])

            columns.append(Column(row[0], coltype, is_nullable, default, row[5]))

        constraints = []
        # Foreign keys
        cur.execute(
            "SELECT name, delete_referential_action, update_referential_action, object_id, referenced_object_id FROM sys.foreign_keys WHERE parent_object_id = %s", oid)

        all_fks = [(row[0], act_mapping[row[1]], act_mapping[row[2]], row[3], row[4]) for row in cur.fetchall()]
        for fk in all_fks:
            fk_name, ondelete, onupdate, fk_id, fk_table_id = fk
            cur.execute(
                "SELECT cc.parent_column_id, ref.name FROM sys.foreign_key_columns cc JOIN sys.columns ref ON (cc.referenced_column_id = ref.column_id) WHERE cc.constraint_object_id = %s AND ref.object_id = %s", (fk_id, fk_table_id))
            tab_columns, ref_columns = [], []
            for row in cur.fetchall():
                tab_columns += [col.name for col in columns if col.num == row[0]]
                ref_columns += [row[1]]

            cur.execute("SELECT name from sys.objects WHERE object_id = %s", fk_table_id)
            reftab_name = cur.fetchone()[0]

            constraints.append(ForeignKey(fk_name, reftab_name, (tab_columns, ref_columns), ondelete, onupdate))

        # primary key/unique constraints, indexes

        cur.execute(
            "SELECT name, index_id, is_primary_key, is_unique_constraint FROM sys.indexes WHERE object_id = %s AND name IS NOT NULL", oid)

        all_keys = [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]

        for key in all_keys:
            # for some reason ODBC returns them as strings sometimes. That's not what we expect
            kname, kid, pk, uni = key
            kid = int(kid)
            pk = int(pk)
            uni = int(uni)
            cur.execute(
                "SELECT column_id FROM sys.index_columns WHERE object_id = %s AND index_id = %s ORDER BY index_column_id", (oid, kid))
            kcols = []
            for row in cur.fetchall():
                kcols += [col.name for col in columns if row[0] == col.num]
            if pk:
                constraints.append(PrimaryKey(kname, kcols))
            elif uni:
                constraints.append(UniqueKey(kname, kcols))
            else:
                indexes.append(Index(kname, kcols))

        # The hack around hack that emulates unique constraints on nullable fields
        uniq_view_name_prefix = 'VIEW_' + name + '_CONSTRAINT_UNIQ_'
        cur.execute("SELECT name, object_id FROM sys.views WHERE name like %s", (uniq_view_name_prefix + '%'))

        fake_uniqs = [(row[0][len(uniq_view_name_prefix):], row[1]) for row in cur.fetchall()]

        for uniq in fake_uniqs:
            uname, uid = uniq
            cur.execute("SELECT name FROM sys.columns WHERE object_id = %s ORDER BY column_id", uid)
            constraints.append(UniqueKey(uname, [row[0] for row in cur.fetchall()]))

        cur.execute(
            "SELECT name, CAST (definition AS VARCHAR(4096)) FROM sys.check_constraints WHERE parent_object_id = %s", oid)
        constraints += [Check(row[0], row[1]) for row in cur.fetchall()]

        return Table(name, columns, constraints, indexes)
    finally:
        cur.close()


def dropConstraint(tab, name, con):
    cur = con.cursor()
    try:
        constr = findFirst(tab.constraints, lambda c: c.name == name)
        if constr is None:
            raise Exception("Cannot drop constraint '%s' on table '%s' - it does not exist" % (name, tab.name))
        view_name = 'VIEW_' + tab.name + '_CONSTRAINT_UNIQ_' + name
        cur.execute("SELECT object_id FROM sys.objects WHERE name = %s", view_name)
        if cur.fetchone():
            # This is an emulation of unique constraint.
            uLogging.debug("Dropping view %s that emulates unique constraint", view_name)
            cur.execute("DROP VIEW %s" % view_name)
        else:
            cur.execute("ALTER TABLE %s DROP CONSTRAINT %s" % (tab.name, name))
    finally:
        cur.close()


def addConstraint(tab, constr, con):
    cur = con.cursor()
    try:
        if isinstance(constr, UniqueKey) and findFirst(constr.columns, lambda c: tab.getColumn(c).nullable) is not None:
            # we should emulate it - see src/library/dbmsspecific/mssql/mssql_ddl.cpp
            aname = tab.name + '_CONSTRAINT_UNIQ_' + constr.name

            uLogging.debug("Creating unique constraint emulation %s", aname)

            cur.execute("CREATE VIEW VIEW_%s WITH SCHEMABINDING AS SELECT %s FROM dbo.%s WHERE %s" % (
                aname, ', '.join(constr.columns), tab.name, ' AND '.join([c + ' IS NOT NULL' for c in constr.columns])))
            cur.execute("CREATE UNIQUE CLUSTERED INDEX INDEX_%s ON VIEW_%s (%s)" %
                        (aname, aname, ', '.join(constr.columns)))
        else:
            cur.execute(constr.getStatement(tab.name))
    finally:
        cur.close()


def dropIndex(table, name, con):
    cur = con.cursor()
    try:
        cur.execute("DROP INDEX %s ON %s" % (name, table.name))
    finally:
        cur.close()


def createIndex(table, index, con):
    cur = con.cursor()
    stmt = "CREATE INDEX %s ON %s(%s)" % (index.name, table.name, ', '.join(index.columns))
    cur.execute(stmt)


def createTable(tab, con):
    cur = con.cursor()
    try:
        stmt = "CREATE TABLE %s (%s)" % (tab.name, ',\n'.join(
            [x.descr(tab) for x in tab.columns + [c for c in tab.constraints if not isinstance(c, UniqueKey)]]))
        cur.execute(stmt)

        for c in tab.constraints:
            if isinstance(c, UniqueKey):
                addConstraint(tab, c, con)

    finally:
        cur.close()


def dropTable(tab, con):
    try:
        cur = con.cursor()
        cur.execute("DROP TABLE %s" % tab.name)
    finally:
        cur.close()


def changeColumnType(tab, name, new_type, con):
    try:
        cur = con.cursor()
        column = tab.getColumn(name)
        default = column.default
        # W/A "default_constraint_name" depends on column error.
        if default is not None:
            setDefault(tab, name, None, con)
        if column is None:
            raise Exception("%s: there is no such column", name)
        cur.execute("ALTER TABLE %s ALTER COLUMN %s %s %s" %
                    (tab.name, name, new_type.MSSQL_name(), column.nullable and "NULL" or "NOT NULL"))
        if default:
            setDefault(tab, name, default, con)
    finally:
        cur.close()


def dropColumnDefault(oid, tname, name, cur):
    cur.execute(
        "SELECT dc.name FROM sys.default_constraints dc JOIN sys.columns c ON (c.column_id = dc.parent_column_id AND c.object_id = dc.parent_object_id) WHERE c.object_id = %s AND c.name = %s", (oid, name))

    default_constraints = [row[0] for row in cur.fetchall()]

    for dc in default_constraints:
        uLogging.debug("Dropping default constraint %s on column %s", dc, name)
        cur.execute("ALTER TABLE %s DROP CONSTRAINT %s" % (tname, dc))


def dropColumn(tab, name, con):
    cur = con.cursor()
    # the moment of excitement
    try:
        cur.execute("SELECT object_id FROM sys.objects WHERE name = %s", tab.name)
        row = cur.fetchone()
        if not row:
            raise Exception, "%s: table unexpectedly disappeared" % tab.name
        oid = row[0]
        cur.execute("SELECT name FROM sys.check_constraints WHERE parent_object_id = %%s AND definition LIKE '%s' ESCAPE '!'" % (
            '%%![' + name + '!]%%',), oid)

        check_constraints = [row[0] for row in cur.fetchall()]

        for chk in check_constraints:
            uLogging.debug("Dropping check constraint %s because it depends on column %s", chk, name)
            cur.execute("ALTER TABLE %s DROP CONSTRAINT %s" % (tab.name, chk))

        dropColumnDefault(oid, tab.name, name, cur)

        for iname in [idx.name for idx in tab.indexes if name in idx.columns]:
            uLogging.debug("Dropping index %s because it depends on column %s", iname, name)
            dropIndex(tab, iname, con)
        for constr in [c for c in tab.constraints if isinstance(c, Key) and name in c.columns]:
            uLogging.debug("Dropping %s %s, because it depends on column %s", constr.kind(), constr.name, name)
            dropConstraint(tab, constr.name, con)
        for constr in [c for c in tab.constraints if isinstance(c, ForeignKey) and name in c.columns[0]]:
            uLogging.debug("Dropping %s %s, because it depends on column %s", constr.kind(), constr.name, name)
            dropConstraint(tab, constr.name, con)

        cur.execute("ALTER TABLE %s DROP COLUMN %s" % (tab.name, name))
    finally:
        cur.close()


def _defaultDesc(defval):
    if defval is None:
        return ''
    elif isinstance(defval, uDBTypes.DefSysDate):
        return 'DEFAULT (getdate())'
    else:
        return 'DEFAULT %s' % defval


def addColumn(tab, column, con):
    cur = con.cursor()
    try:
        cur.execute("ALTER TABLE %s ADD [%s] %s %s %s" % (
            tab.name, column.name, column.type.MSSQL_name(), not column.nullable and 'NOT NULL' or '', _defaultDesc(column.default)))
    finally:
        cur.close()


def renameTable(tab, new_name, con):
    cur = con.cursor()
    cur.execute("exec sys.sp_rename @objname='%s', @newname='%s'" % (tab.name, new_name))


def renameColumn(tab, old_name, new_name, con):
    cur = con.cursor()
    cur.execute("exec sys.sp_rename @objname='%s.[%s]', @newname='%s',  @objtype = 'COLUMN' " % (
        tab.name, old_name, new_name))


def findFirst(seq, criterion=None):
    if criterion is None:
        criteterion = lambda x: x
    for item in seq:
        if criterion(item):
            return item
    return None


def changeNullable(table, cname, notnullable, con):
    cur = con.cursor()

    try:
        column = findFirst(table.columns, lambda x: x.name == cname)
        if column is None:
            raise Exception("Table %s does not have column %s" % (table.name, cname))

        # drop constraints and indexes which refers to the target column (see #POA-47061 and #POA-75190)
        uniq_constraints = [c for c in table.constraints if isinstance(c, UniqueKey) and c.refers(cname)]
        for c in uniq_constraints:
            table.dropConstraint(c.name, con)

        d_indexes = [idx for idx in table.indexes if idx.refers(cname)]
        for idx in d_indexes:
            table.dropIndex(idx.name, con)

        cur.execute("ALTER TABLE %s ALTER COLUMN %s %s %s NULL" %
                    (table.name, cname, column.type.MSSQL_name(), notnullable and 'NOT' or ''))
        table.reinit(con)

        for idx in d_indexes:
            table.addIndex(idx, con)

        for c in uniq_constraints:
            table.addConstraint(c, con)

    finally:
        cur.close()


def setDefault(table, column, default, con):
    cur = con.cursor()
    try:
        cur.execute("SELECT object_id FROM sys.objects WHERE name = %s", table.name)
        oid = cur.fetchone()[0]
        dropColumnDefault(oid, table.name, column, cur)
        if default is not None:
            constraint_default = table.name + '_' + column + '__dfl_'
            cur.execute("ALTER TABLE %s ADD CONSTRAINT %s %s FOR %s" %
                        (table.name, constraint_default, _defaultDesc(default), column))
    finally:
        cur.close()


def columnDescription(col, table):
    props = {"name": col.name, "null": not col.nullable and "NOT NULL" or "", "type": col.type.MSSQL_name()}
    if col.default is not None:
        props['def'] = _defaultDesc(col.default)
    else:
        props['def'] = ''

    return "[%(name)s] %(type)s %(def)s %(null)s" % props


def identityInsertBegin(table, con):
    cur = con.cursor()
    cur.execute("SET IDENTITY_INSERT %s ON" % table.name)
    cur.close()


def identityInsertEnd(table, con):
    cur = con.cursor()
    cur.execute("SET IDENTITY_INSERT %s OFF" % table.name)
    cur.close()


def exprEq(dummy1, dummy2):
    # TODO
    return True


def getTableList(con):
    cur = con.cursor()
    cur.execute("SELECT name FROM sys.tables WHERE name NOT LIKE 'sql_%' AND name != 'dual'")
    return [row[0] for row in cur.fetchall()]


def dropIdentity(tab, con):
    identity_column = first(tab.columns, lambda x: isinstance(x.type, uDBTypes.SerialType))
    if not identity_column:
        uLogging.err("There is no identity column on table %s", tab.name)
        return
    icname = identity_column.name
    cur = con.cursor()
    oid = _getTableOid(tab.name, cur)
    cur.execute(
        "SELECT ft.name, fk.name FROM sys.foreign_keys fk JOIN sys.objects ft ON (ft.object_id = fk.parent_object_id) WHERE fk.referenced_object_id = %s", oid)

    fks = [(getTable(row[0], con), row[1]) for row in cur.fetchall()]
    fks = [(x[0], x[0].getConstraint(x[1])) for x in fks]

    for ft, fk in fks:
        ft.dropConstraint(fk.name, con)

    constraints = [x for x in tab.constraints if x.refers(icname)]
    for c in constraints:
        tab.dropConstraint(c.name, con)

    was_nullable = identity_column.nullable
    new_column = Column(icname, uDBTypes.IntType(), True)
    indices = [x for x in tab.indexes if x.refers(icname)]
    for i in indices:
        tab.dropIndex(i.name, con)
    tab.renameColumn(icname, icname + '_old', con)
    tab.addColumn(new_column, con)
    cur.execute("UPDATE %s SET %s = %s" % (tab.name, icname, icname + '_old'))
    tab.dropColumn(icname + '_old', con)

    if not was_nullable:
        tab.dropNullable(icname, con)

    for c in constraints:
        tab.addConstraint(c, con)

    for i in indices:
        tab.addIndex(i, con)

    for ft, fk in fks:
        ft.addConstraint(fk, con)


def escapedList(ls):
    return ', '.join(['[%s]' % x for x in ls])


def keyDescription(key):
    return "CONSTRAINT %s %s (%s)" % (key.name, key.kind(), escapedList(key.columns))
__all__ = ['getTable']


def fkDescription(fk):
    return "CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE %s" % (fk.name, escapedList(fk.columns[0]), fk.reftable, escapedList(fk.columns[1]), fk.ondelete)
