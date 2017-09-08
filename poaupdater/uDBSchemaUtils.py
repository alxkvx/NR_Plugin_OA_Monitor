__rcs_id__ = """$Id$"""

__pychecker__ = 'unusednames=__rcs_id__,dummy'
import uSysDB
import uDBSchema
import uDialog
import uDBTypes
import uLogging


def dropConstraintIfExists(table, constraint, connection=None):
    if not connection:
        con = uSysDB.connect()
        own = True
    else:
        con = connection
        own = False

    if isinstance(table, uDBSchema.Table):
        tab = table
    else:
        tab = uDBSchema.getTable(table, con)

    if tab.getConstraint(constraint):
        uLogging.info("DROP CONSTRAINT %s ON %s", constraint, tab.name)
        tab.dropConstraint(constraint, con)
        if own:
            con.commit()
    else:
        uLogging.warn(
            "Constraint %s does not exist on table %s. But it's ok, it should be dropped anyway", constraint, tab.name)


def dropColumnIfExists(table, column, connection=None):
    if not connection:
        con = uSysDB.connect()
        own = True
    else:
        con = connection
        own = False

    if isinstance(table, uDBSchema.Table):
        tab = table
    else:
        tab = uDBSchema.getTable(table, con)

    if tab.getColumn(column):
        tab.dropColumn(column, con)
        if own:
            con.commit()
    else:
        uLogging.warn(
            "Column %s does not exist on table %s. But it's ok, it should be dropped anyway", column, tab.name)


def dropIndexIfExists(table, index, connection=None):
    if not connection:
        con = uSysDB.connect()
        own = True
    else:
        con = connection
        own = False

    if isinstance(table, uDBSchema.Table):
        tab = table
    else:
        tab = uDBSchema.getTable(table, con)

    if tab.getIndex(index):
        uLogging.info("DROP INDEX %s ON %s", index, tab.name)
        tab.dropIndex(index, con)
        if own:
            con.commit()
    else:
        uLogging.warn("Index %s does not exist on table %s. But it's ok, it should be dropped anyway", index, tab.name)


def dropTableIfExists(table, connection=None):
    if not connection:
        con = uSysDB.connect()
        own = True
    else:
        con = connection
        own = False

    if isinstance(table, uDBSchema.Table):
        tab = table
    else:
        try:
            tab = uDBSchema.getTable(table, con)
        except uDBSchema.NoSuchTable:
            uLogging.info('%s does not exist. But ok, it should be dropped anyway', table)
            return
    uLogging.info('DROP TABLE %s', tab.name)
    tab.drop(con)

    if own:
        con.commit()


def generateCondition(columns, values):
    eq_str = ""
    cond_params = []

    for cname, value in zip(columns, values):
        if eq_str:
            eq_str += ' AND '
        eq_str += cname
        if value is None:
            eq_str += ' IS NULL '
        else:
            eq_str += ' = %s '
            cond_params.append(value)
    return eq_str, tuple(cond_params)


def generateNegativeCondition(columns, values):
    eq_str = ""
    cond_params = []

    for cname, value in zip(columns, values):
        if eq_str:
            eq_str += ' OR '
        eq_str += cname
        if value is None:
            eq_str += ' IS NOT NULL '
        else:
            eq_str += ' != %s OR ' + cname + ' IS NULL'
            cond_params.append(value)
    return eq_str, tuple(cond_params)


def removeDuplicates(table, con):
    # XXX: hack, truncate all timestamp columns to 1 second precision for
    # postgres, because PyPgSQL datetime does not hold enough data to make
    # correct equal comparisions with timestamp
    if uSysDB.DBType == uSysDB.PgSQL:
        dt_column_names = [c.name for c in table.columns if isinstance(c.type, uDBTypes.TimestampType)]
        if dt_column_names:
            query = "UPDATE %s SET " % table.name
            query += ', '.join(["%s = date_trunc('seconds', %s)" % (x, x) for x in dt_column_names])
            cur = con.cursor()
            cur.execute(query)

    columns_str = ', '.join([str(c.name) for c in table.columns])
    column_names = [c.name for c in table.columns]
    columns_placeholders_str = ','.join(['%s'] * len(table.columns))
    cur = con.cursor()
    cur.execute("SELECT %(columns)s FROM %(tab)s GROUP BY %(columns)s HAVING COUNT(*) > 1" %
                {'columns': columns_str, 'tab': table.name})

    upd_cur = con.cursor()
    for row in cur.fetchall():
        uLogging.debug("Removing duplicate records %s", row)
        cond, params = generateCondition(column_names, row)
        row = tuple(row)
        upd_cur.execute(("DELETE FROM %s WHERE %s" % (table.name, cond)), params)
        upd_cur.execute(("INSERT INTO %s (%s) VALUES (%s)" % (table.name, columns_str, columns_placeholders_str)), row)


def _oneUniquePhase(table, con, columns, other_columns):
    uniq_columns_str = ', '.join(columns)

    other_column_names = [c.name for c in other_columns]
    other_columns_str = ', '.join(other_column_names)

    cur = con.cursor()
    cur.execute("SELECT %(columns)s FROM %(tab)s GROUP BY %(columns)s HAVING COUNT(*) > 1" %
                {'columns': uniq_columns_str, 'tab': table.name})

    l_cur = con.cursor()
    rows = [row for row in cur.fetchall()]
    for row in rows:
        comm_cond, comm_params = generateCondition(columns, [x for x in row])
        l_cur.execute(("SELECT %(other)s FROM %(tab)s WHERE %(cond)s " %
                       {"other": other_columns_str, 'tab': table.name, 'cond': comm_cond}), comm_params)

        not_uniq_rows = l_cur.fetchall()
        uLogging.info('There are %s records in table %s with same %s (%s), but with different %s\n', len(
            not_uniq_rows), table.name, uniq_columns_str,  ', '.join([str(i) for i in row]), other_columns_str)
        uLogging.info(
            "(%s) should be unique, please select one row that should be kept intact (others will be deleted), or press 'a' to abort. Also, you can fix the issue manually and press 'r'\n", uniq_columns_str)

        choice_map = {}
        rows_map = {}
        for i in xrange(len(not_uniq_rows)):
            choice_map[str(i)] = 'Keep row %s\n' % ', '.join(['%s=%s' % (x[0].name, str(x[1]))
                                                              for x in zip(other_columns, not_uniq_rows[i])])
            rows_map[str(i)] = not_uniq_rows[i]

        choice_map['a'] = 'Abort'
        choice_map['r'] = 'Retry'

        answer = uDialog.select([' ', 'What should I do?'], choice_map)
        if answer == 'a':
            raise Exception("Duplicate records in table %s" % table.name)
        elif rows_map.has_key(answer):
            left_cond, left_params = generateNegativeCondition(other_column_names, [x for x in rows_map[answer]])
            statement = "DELETE FROM %(tab)s WHERE %(cond)s AND (%(left_cond)s)" % {
                'tab': table.name, 'cond': comm_cond, 'left_cond': left_cond}
            uLogging.debug("%s (%s, %s)", statement, comm_params, left_params)
            l_cur.execute(statement, comm_params + left_params)
            con.commit()
        else:
            return False
    return True


def ensureUniqueness(table, con, columns):
    removeDuplicates(table, con)

    con.commit()

    other_columns = [c for c in table.columns if c.name not in columns]
    if not other_columns:
        return

    while not _oneUniquePhase(table, con, columns, other_columns):
        pass


__all__ = ["dropConstraintIfExists", "dropIndexIfExists", "dropTableIfExists", 'removeDuplicates']


def turnIndexToUniq(tname, idxname):
    if uSysDB.DBType != uSysDB.PgSQL:
        return
    con = uSysDB.connect()
    tab = uDBSchema.getTable(tname, con)
    idx = tab.getIndex(idxname)
    if idx is None:
        uLogging.debug("%s: no such index, it's ok", idxname)
        return
    cur = con.cursor()
    cur.execute("SELECT oid FROM pg_class WHERE relname = %s", tname)
    row = cur.fetchone()
    if not row:
        uLogging.err("Table %s does not exist", tname)
        return

    toid = row[0]
    cur.execute(
        "SELECT relname, conname FROM pg_constraint c JOIN pg_class r ON (r.oid = c.conrelid) WHERE c.confrelid = %s", toid)

    fks = [(uDBSchema.getTable(row[0], con), row[1]) for row in cur.fetchall()]
    fks = [(x[0], x[0].getConstraint(x[1])) for x in fks]

    for t, fk in fks:
        uLogging.debug("Temporarily dropping %s", fk)
        t.dropConstraint(fk.name, con)
    tab.dropIndex(idxname, con)
    tab.addConstraint(uDBSchema.UniqueKey(idxname, idx.columns), con)

    for t, fk in fks:
        uLogging.debug("Restoring %s", fk)
        t.addConstraint(fk, con)
