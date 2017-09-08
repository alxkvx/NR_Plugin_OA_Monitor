__rcs_id__ = """$Id$"""
import re

from uDBSchema import *
import uDBTypes
import uPostgreSQLTypes
from uGenerics import first
import  uSysDB, uLogging

# uDBSchemaPgSQL  module should never be imported directly. it is
# implementation of PgSQL specific part of uDBSchema module


def seq(name):
    return "%s_seq" % name


def quotjoin(a):
    return ', '.join(['`%s`' % c for c in a])


def getColumnType(attypid, attlen, attypmod):
    if attypid in uPostgreSQLTypes.char_types:
        if attypid == uPostgreSQLTypes.PG_TYPE_BPCHAR:
            return uDBTypes.CharType(attypmod - 4)
        else:
            return uDBTypes.CharType(attlen)
    elif attypid == uPostgreSQLTypes.PG_TYPE_VARCHAR:
        return uDBTypes.VarcharType(attypmod - 4)
    elif attypid in uPostgreSQLTypes.blob_types:
        return uDBTypes.BlobType()
    elif attypid == uPostgreSQLTypes.PG_TYPE_TEXT:
        return uDBTypes.VarcharType(attlen)
    elif attypid in uPostgreSQLTypes.int_types:
        return uDBTypes.IntType()
    elif attypid == uPostgreSQLTypes.PG_TYPE_INT8:
        return uDBTypes.BigIntType()
    elif attypid == uPostgreSQLTypes.PG_TYPE_NUMERIC:
        return uDBTypes.NumericType(((attypmod - 4) >> 16) & 65535, (attypmod - 4) & 65535)
    elif attypid in uPostgreSQLTypes.timestamp_types:
        return uDBTypes.TimestampType()
    else:
        return uDBTypes.Type(attypid, attlen, attypmod)


def get_public_ns(cur):
    cur.execute("SELECT oid::int FROM pg_namespace WHERE nspname = 'public'")
    return cur.fetchone()[0]


def _getTable(name, con):
    def _getForeignKey(conname, confrelid, confupdtype, confdeltype, conkey, confkey):
        constr_columns = []
        for num in conkey.split():
            num = int(num)
            constr_columns.extend([x.name for x in columns if x.num == num])


        cur.execute("SELECT attname FROM pg_attribute WHERE attrelid = %s AND attnum > 0 ORDER BY attnum", confrelid)
        foreign_table_columns = [x[0] for x in cur.fetchall()]

        fk_columns = [foreign_table_columns[int(x) - 1] for x in confkey.split()]
        cur.execute("SELECT relname FROM pg_class WHERE oid = %s AND relnamespace = %s", confrelid, public_ns)
        ft_name = cur.fetchone()[0]
        onupdate = act_mapping[confupdtype]
        ondelete = act_mapping[confdeltype]
        return ForeignKey(conname, ft_name, (constr_columns, fk_columns), ondelete, onupdate)

    cur = con.cursor()
    columns = []
    public_ns = get_public_ns(cur)
    cur.execute("SELECT oid::int FROM pg_class WHERE relname = %s AND relnamespace = %s", name, public_ns)
    row = cur.fetchone()
    if not row:
        raise NoSuchTable(name)
    oid = row[0]

    cur.execute(
        "SELECT attname, atttypid::int, attlen, atttypmod, attnotnull,attnum, atthasdef FROM pg_attribute WHERE attrelid = %s AND attnum > 0 AND NOT attisdropped ORDER by attnum", oid)

    rows = cur.fetchall()
    for row in rows:
        coltype = getColumnType(row[1], row[2], row[3])
        if row[6]:
            # pg_attrdef contains incorrect value of sequence name if the sequence was renamed, thus use information_schema
            cur.execute("SELECT column_default FROM information_schema.columns WHERE table_name = %s and column_name = %s", (name, row[0]))

            default = cur.fetchone()[0]
            if default.startswith('nextval('):
                # not very correct, but correct for
                # PEM created tables.
                coltype = uDBTypes.SerialType()
            elif default == 'now()':
                default = uDBTypes.DefSysDate()

        else:
            default = None
        columns.append(Column(row[0], coltype, not row[4], default, row[5]))
    cur.execute("SELECT conname, contype::text, confrelid::int, confupdtype::text, confdeltype::text, array_to_string(conkey, ' '), array_to_string(confkey, ' '), consrc FROM pg_constraint WHERE conrelid = %s", oid)
    rows = cur.fetchall()
    constraints = []
    for row in rows:
        contype = row[1]
        if contype in ('c', 'p', 'u', 'f'):
            constr_columns = []
            for num in row[5].split():
                num = int(num)
                constr_columns += [x.name for x in columns if x.num == num]
            if contype == 'p':
                constraints.append(PrimaryKey(row[0], constr_columns))
            elif contype == 'u':
                constraints.append(UniqueKey(row[0], constr_columns))
            elif contype == 'c':
                constraints.append(Check(row[0], row[7]))
            else:
                constraints.append(_getForeignKey(row[0], row[2], row[3], row[4], row[5], row[6]))

    # Get list of "Referenced by" Tables and FK
    cur.execute("SELECT conname, contype::text, conrelid::int, confupdtype::text, confdeltype::text, array_to_string(conkey, ' '), array_to_string(confkey, ' '), consrc FROM pg_constraint WHERE confrelid = %s", oid)
    rows = cur.fetchall()
    referenced_fk = []
    for row in rows:
        referenced_fk.append(_getForeignKey(row[0], row[2], row[3], row[4], row[6], row[5]))

    cur.execute("SELECT array_to_string(indkey, ' '), indexrelid::int, indisunique FROM pg_index WHERE indrelid = %s", oid)
    rows = cur.fetchall()
    indexes = []
    for row in rows:
        cur.execute("SELECT relname FROM pg_class WHERE oid = %s AND relnamespace = %s", row[1], public_ns)
        indname = cur.fetchone()[0]
        same_constr = [x for x in constraints if x.name == indname]
        if same_constr:
            continue
        ind_columns = []
        for num in row[0].split():
            ind_columns += [x.name for x in columns if x.num == int(num)]
        indexes.append(Index(indname, ind_columns, row[2]))

    cur.close()
    return Table(name, columns, constraints, indexes, referenced_fk)

act_mapping = {
    'a': "NO ACTION",
    'c': "CASCADE",
    'r': "RESTRICT",
    'n': "SET NULL"

}


def dropConstraint(tab, name, con):
    cur = con.cursor()
    cur.execute("ALTER TABLE `%s` DROP CONSTRAINT `%s` CASCADE" % (tab.name, name))


def addConstraint(tab, constr, con):
    cur = con.cursor()
    try:
        cur.execute(constr.getStatement(tab.name))
    finally:
        cur.close()


def createTable(tab, con):
    cur = con.cursor()
    for c in [x for x in tab.columns if isinstance(x.type, uDBTypes.SerialType)]:
        cur.execute("CREATE SEQUENCE `%s` START WITH %s" % (seq(tab.name), c.type.startswith))

    stmt = "CREATE TABLE `%s` (%s) WITH OIDS" % (
        tab.name, ',\n'.join([x.descr(tab) for x in tab.columns + tab.constraints]))

    cur.execute(stmt)
    cur.close()


def dropTable(tab, con):
    cur = con.cursor()
    identity_columns = get_identity_columns(tab.name, con)

    cur.execute("DROP TABLE `%s`" % tab.name)

    for column in identity_columns:
        if uSysDB.table_exist(con, column.sequence):
            cur.execute("DROP SEQUENCE `%s`" % column.sequence)

    cur.close()


def changeColumnType(tab, name, new_type, con):
    cur = con.cursor()
    cur.execute("ALTER TABLE `%s` ALTER `%s` TYPE %s" % (tab.name, name, new_type.PgSQL_name()))
    cur.close()


def dropColumn(tab, name, con):
    cur = con.cursor()
    cur.execute("ALTER TABLE `%s` DROP COLUMN `%s`" % (tab.name, name))
    for x in tab.columns:
        if x.name == name and isinstance(x, uDBTypes.SerialType):
            cur.execute("DROP SEQUENCE `%s`" % seq(tab.name))
    cur.close()


def _defaultDesc(defval):
    if defval is None:
        return ''
    elif isinstance(defval, uDBTypes.DefSysDate):
        return 'DEFAULT now()'
    else:
        return 'DEFAULT %s' % defval


def addColumn(tab, column, con):
    cur = con.cursor()
    default_str = _defaultDesc(column.default)
    if isinstance(column.type, uDBTypes.SerialType):
        cur.execute("CREATE SEQUENCE `%s`" % seq(tab.name))
        default_str = "DEFAULT nextval('`%s`')" % seq(tab.name)
    cur.execute("ALTER TABLE `%s` ADD `%s` %s %s %s" %
                (tab.name, column.name, column.type.PgSQL_name(), not column.nullable and 'NOT NULL' or '', default_str))


def renameTable(tab, new_name, con):
    cur = con.cursor()
    cur.execute("ALTER TABLE `%s` RENAME to `%s`" % (tab.name, new_name))

    for col in tab.columns:
        if isinstance(col.type, uDBTypes.SerialType):
            cur.execute("ALTER SEQUENCE `%s` RENAME TO `%s`" % (seq(tab.name), seq(new_name)))
#			cur.execute("ALTER TABLE %s ALTER %s SET DEFAULT nextval('%s_seq')" % (new_name, col.name, new_name))


def changeNullable(table, column, notnullable, con):
    cur = con.cursor()
    cur.execute("ALTER TABLE `%s` ALTER `%s` %s NOT NULL" % (table.name, column, notnullable and 'SET' or 'DROP'))
    cur.close()


def setDefault(table, column, default, con):
    cur = con.cursor()
    try:
        if default is None:
            cur.execute("ALTER TABLE `%s` ALTER `%s` DROP DEFAULT" % (table.name, column))
        elif isinstance(default, uDBTypes.DefSysDate):
            cur.execute("ALTER TABLE `%s` ALTER `%s` SET DEFAULT now()" % (table.name, column))
        else:
            cur.execute("ALTER TABLE `%s` ALTER `%s` SET DEFAULT %s" % (table.name, column, default))

    finally:
        cur.close()


def dropIndex(dummy, name, con):
    cur = con.cursor()
    cur.execute("DROP INDEX `%s`" % name)


def createIndex(table, index, con):
    cur = con.cursor()

    UNIQUE = ''
    if index.unique:
        UNIQUE='UNIQUE'

    stmt = "CREATE %s INDEX `%s` ON `%s`(%s)" % (UNIQUE, index.name, table.name, quotjoin(index.columns))
    cur.execute(stmt)


def renameColumn(table, old_name, new_name, con):
    cur = con.cursor()
    stmt = "ALTER TABLE `%s` RENAME `%s` TO `%s`" % (table.name, old_name, new_name)
    cur.execute(stmt)


def columnDescription(col, table):
    props = {"name": col.name, "null": not col.nullable and "NOT NULL" or "", "type": col.type.PgSQL_name()}
    if col.default is not None and isinstance(col.default, uDBTypes.DefSysDate):
        props['def'] = 'DEFAULT now()'
    elif isinstance(col.type, uDBTypes.SerialType):
        props['def'] = "DEFAULT nextval('`%s`')" % seq(table.name)
    elif col.default is not None:
        props['def'] = "DEFAULT %s" % col.default
    else:
        props['def'] = ''

    return "`%(name)s` %(type)s %(def)s %(null)s" % props


def identityInsertBegin(dummy, dummy2):
    pass


def identityInsertEnd(table, con):
    for column in get_identity_columns(table.name, con):
        cur = con.cursor()
        cur.execute("select setval('`{sequence}`', greatest(max(`{column}`)::bigint, last_value)) FROM `{table}`, `{sequence}` group by last_value".format(
            sequence=column.sequence, table=table.name, column=column.name))
        cur.close()


def get_identity_columns(table, con):
    tab = getTable(table, con)
    columns = []
    for col in tab.columns:
        if isinstance(col.type, uDBTypes.SerialType):
            m = re.match("nextval\('\"?([^\"]+)\"?'", col.default)
            if m:
                col.sequence = m.group(1)
                columns.append(col)
            else:
                #				uLogging.warn('Strange Serial type of column {}.{} with default {}'.format(table, col.name, col.default))
                uLogging.warn('Strange Serial type of column %s.%s with default %s' % (table, col.name, col.default))
    if len(columns) > 1:
        uLogging.warn('Strange condition: more then one column with autoincrenet at table {}'.forma(table))

    return columns


def dropIdentity(tab, con):
    identity_column = first(tab.columns, lambda x: isinstance(x.type, uDBTypes.SerialType))
    if identity_column is None:
        uLogging.err("There is no identity column on table %s", tab.name)
        return

    cur = con.cursor()
    cur.execute("ALTER TABLE `%s` ALTER `%s` DROP DEFAULT" % (tab.name, identity_column.name))
    cur.execute("DROP SEQUENCE `%s`" % seq(tab.name))


def getTableList(con):
    cur = con.cursor()
    public_ns = get_public_ns(cur)
    cur.execute(
        "SELECT relname FROM pg_class WHERE relkind = 'r' AND relname NOT like 'pg_%%' AND relname != 'dual' AND relnamespace = %s", public_ns)
    return [row[0] for row in cur.fetchall()]


def exprEq(dummy1, dummy2):
    # TODO
    return True


def keyDescription(key):
    return "CONSTRAINT `%s` %s (%s)" % (key.name, key.kind(), quotjoin(key.columns))


def fkDescription(fk):
    return "CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`(%s) ON DELETE %s" % (fk.name, quotjoin(fk.columns[0]), fk.reftable, quotjoin(fk.columns[1]), fk.ondelete)
__all__ = ['getTable']
