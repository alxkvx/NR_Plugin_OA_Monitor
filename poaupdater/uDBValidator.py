from poaupdater import uPrecheck

__rcs_id__ = """$Id$"""

__pychecker__ = "unusednames=__rcs_id__,dummy"

import cStringIO


import os
import os.path
import sys
import zipfile
import tempfile
import shutil

import uUtil
import uPEM
import uDBSchema
import uDBTypes
import uSysDB
import uManifestParser
import uPDLDBSchema
import uLogging
import uActionContext
import uTextRender
from uBuild import BuiltPackage
from uDBErrors import *
from uAction import progress


def isMissingOrRedundantIndex(index_error):
    return isinstance(index_error, DBErrorRedundantIndex) or isinstance(index_error, DBErrorMissingIndex) or isinstance(index_error, DBErrorNonconformingIndex)


def unzipFile(filename, destination):
    zfile = zipfile.ZipFile(filename)
    for name in zfile.namelist():
        (dirname, filename) = os.path.split(name)
        if filename == '':  # directory
            dn = os.path.join(destination, dirname)
            if not os.path.exists(dn):
                os.mkdir(dn)
        else:  # file
            fn = os.path.join(destination, name)
            fnd = os.path.dirname(fn)
            if not os.path.exists(fnd):
                os.makedirs(fnd)  # make dirs all the way through
            fd = open(fn, 'wb')
            fd.write(zfile.read(name))
            fd.close()


def readTablesFromDBManifests(con):
    progress.do("gathering schema information from manifests (from database)")
    cur = con.cursor()
    cur.execute("""SELECT sc.name, data FROM package_body p
	JOIN components c ON (c.pkg_id = p.pkg_id)
	JOIN sc_instances si ON (si.component_id = c.component_id)
	JOIN service_classes sc ON (sc.sc_id = si.sc_id)
	WHERE sc.name NOT IN ('pleskd', 'vzpemagent')""")

    manifest_tables = dict()
    for row in cur.fetchall():
        data = row[1]
        data = str(data)
        xml_text = uManifestParser.unsign_package(cStringIO.StringIO(data))
        for table in uPDLDBSchema.getSchemaFromString(xml_text):
            manifest_tables[table.name] = table

    # Add tables from ejb's reduced descriptor
    try:
        earFiles = []
        dummy, _mn_plesk_root = uPEM.getMNInfo()
        from u import bootstrap
        for root, subFolders, files in os.walk(bootstrap.getJBossDir(_mn_plesk_root) + '/standalone/data/content/'):
            for file in files:
                earFiles.append(os.path.join(root, file))

        wDir = tempfile.mkdtemp()
        for earFile in earFiles:
            progress.do("unzip ear file " + earFile + " to " + wDir)
            unzipFile(earFile, wDir)
            progress.done()

        jarFiles = [f for f in os.listdir(wDir) if f.endswith(".jar")]
        for jarFile in jarFiles:
            wDirJar = tempfile.mkdtemp()
            progress.do("unzip jar file " + jarFile + " to " + wDirJar)
            unzipFile(os.path.join(wDir, jarFile), wDirJar)
            progress.done()
            rdFilePath = os.path.join(wDirJar, 'rd.xml')
            if not os.path.isfile(rdFilePath):
                continue
            rdFile = open(rdFilePath, 'r')
            rdContent = rdFile.read()
            rdFile.close()
            for table in uPDLDBSchema.getSchemaFromString(rdContent):
                manifest_tables[table.name] = table

            shutil.rmtree(wDirJar)

        shutil.rmtree(wDir)

    except Exception, e:
        uUtil.logLastException()
        pass
    except:
        uUtil.logLastException()
        pass

    progress.done()
    return manifest_tables


def readTablesFromManifestFiles(build_info):
    progress.do("gathering schema information from manifests (from files)")

    manifest_tables = dict()
    for build in build_info.builds:
        for pkg, built_pkg in build.contents.iteritems():
            if isinstance(built_pkg, BuiltPackage):
                xml_text = uManifestParser.unsign_package(built_pkg.manifest_file)
                for table in uPDLDBSchema.getSchemaFromString(xml_text):
                    manifest_tables[table.name] = table

    progress.done()

    return manifest_tables


def readTablesFromDB(con):
    progress.do("gathering real database schema")
    tablist = uDBSchema.getTableList(con)
    tables = dict()
    for tname in tablist:
        tables[tname] = uDBSchema.getTable(tname, con)

    progress.done()

    return tables


#LISA and Billing upgrade scripts can generate different names for constraints,
#for example LISA "MSRPPrices_PK", and upgade scrips "MSRPPrices_AccountID_PK",
#Thus, we have to skip constraints name comparasion during Billing DB verification

#set skip_constraint_names = True for skip constraint's names comparasion (for Billing)
def validateDatabaseExt(manifest_tables, tables, skip_constraint_names = False):
    validator = DBValidator(skip_constraint_names)
    progress.do("checking for missing tables")
    missing = validator.searchMissingTables(manifest_tables, tables)
    progress.done()
    progress.do("checking for redundant tables")
    redundant = validator.searchRedundantTables(manifest_tables, tables)
    progress.done()
    progress.do("validating table structure")
    invalid = validator.validateTables(manifest_tables, tables)
    progress.done()
    return missing, invalid, redundant
    

def validateDatabase():
    uDBSchema.Table.reset_cache()  # Need to get actual data in retriable actions
    con = uSysDB.connect()
    manifest_tables = readTablesFromDBManifests(con)
    tables = readTablesFromDB(con)

    return validateDatabaseExt(manifest_tables, tables)

def repairDatabase(build_info=None):
    con = uSysDB.connect()
    if None == build_info:
        # read manifest schema from DB
        manifest_tables = readTablesFromDBManifests(con)
    else:
        # get it from on-disk manifests
        manifest_tables = readTablesFromManifestFiles(build_info)

    tables = readTablesFromDB(con)

    validator = DBValidator()
    progress.do("repairing tables")
    errors = validator.repairTables(con, manifest_tables, tables)
    progress.done()

    return errors


def getMissingIdList(con, child_table, child_column, parent_table, parent_column, additional_where=None):
    """
    This function silently compares two tables and finds in child table all items
    that reference entities missing in parent table and yields a list of missed id.
    :param con: a database connection
    :param child_table: child table that contains items referencing entities in parent table
    :param child_column: column in child table referencing parent column of parent table
    :param parent_table: parent table that contains (or may contain) entities referenced by items of child table
    :param parent_column: column in parent table referenced by child column of child table
    :param additional_where:
    """

    if not additional_where:
        additional_where = ""
    else:
        additional_where = "AND %s" % additional_where

    query = """
	SELECT DISTINCT ct.%s FROM %s ct LEFT JOIN %s pt
	ON (ct.%s = pt.%s)
	WHERE pt.%s IS NULL
	%s
	AND ct.%s IS NOT NULL ORDER BY ct.%s
	""" % (child_column, child_table, parent_table, child_column, parent_column, parent_column, additional_where, child_column, child_column)
    cur = con.cursor()
    cur.execute(query)

    missing_ids = [missing_id for (missing_id, ) in cur.fetchall()]
    return missing_ids


def getTableAsText(con, table_name, condition=None, cols_to_show=None):
    headers = []
    text_table = uTextRender.Table()
    if cols_to_show:
        headers = list(cols_to_show)
    else:
        table = uDBSchema.getTable(table_name, con)
        for col in table.columns:
            headers += [col.name]
    text_table.setHeader(headers)
    query = "SELECT %s FROM %s" % (', '.join(headers), table_name)
    if condition:
        query = "%s WHERE %s" % (query, condition)
    cur = con.cursor()
    cur.execute(query)
    for row in cur.fetchall():
        text_table.addRow([str(item).decode('utf-8') for item in row])
    return "%s" % text_table


def getMappedEntities(entity, missing_id_list):
    if entity == 'sub_id' or entity == 'subscription_id':  # check if PBA contains subscriptions not-existing in POA
        query = """SELECT * FROM "Subscription" WHERE "subscriptionID" IN (%s)""" % missing_id_list
        message = """Make sure that Billing doesn't contain any information related to missing values.
You can run the following SQL query in Billing database to check if such entries exist:
 > %s""" % query
        return message
    else:
        return ""


def compareTables(con, child_table, child_column, parent_table, parent_column, throw, cols_to_show=None, recommendation=None, additional_where=None, auto_fix=False):
    """
    This function compares two tables and finds in child table all items
    that reference entities missing in parent table.
    :param con: a database connection
    :param child_table: child table that contains items referencing entities in parent table
    :param child_column: column in child table referencing parent column of parent table
    :param parent_table: parent table that contains (or may contain) entities referenced by items of child table
    :param parent_column: column in parent table referenced by child column of child table
    :param throw: if throw exception or perform silently
    :param additional_where:
    :return: nothing
    """
    missing_ids = getMissingIdList(
        con, child_table, child_column, parent_table, parent_column, additional_where=additional_where)
    if missing_ids:
        in_precheck = uActionContext.in_precheck()
        if in_precheck:
            details = getTableAsText(con, child_table, "%s IN (%s)" % (
                child_column, ', '.join(map(lambda missing_id: str(missing_id), missing_ids))), cols_to_show)
            details = "%sorphaned id-s in column ('%s'): [%s]" % (details, child_column,
                                                                  ', '.join(map(lambda missing_id: str(missing_id), missing_ids)))
        else:
            details = "%s: %s" % (child_column, ', '.join(map(lambda missing_id: str(missing_id), missing_ids)))
        info_str = """
		Column '%s.%s' contains values that reference entities missing in column '%s.%s'.
		Data in table '%s' require modification.
		Items that reference not-existing parent entities:
		%s""" % (child_table, child_column, parent_table, parent_column, child_table, details )
        if not in_precheck and auto_fix:
            info_str += """
		If this issue is ignored, the Operation Automation updater will attempt to fix it automatically, but still the positive result is not guaranteed."""
        if throw:
            pba_message = getMappedEntities(
                parent_column, ', '.join(map(lambda missing_id: str(missing_id), missing_ids)))
            if recommendation:
                what_to_do = "%s" % recommendation
            else:
                what_to_do = "remove records from table %s, that are referencing missing values in table %s.\n%s" % (
                    child_table, parent_table, pba_message)
            raise uPrecheck.PrecheckFailed(info_str, what_to_do)
        else:
            what_to_do = "\nSystem will remove records, that are referencing missing values, automatically during upgrade"
            uLogging.warn(info_str + what_to_do)


def correctTable(con, table, column, parent_table, parent_column):
    """
    Correct (child) table by deleting items with orphaned ids.
    :param con:
    :param table:
    :param column:
    :param parent_table: parent table that contains (or may contain) entities referenced by items of child table
    :param parent_column: column in parent table referenced by child column of child table
    """
    orphaned_ids = getMissingIdList(con, table, column, parent_table, parent_column)
    if orphaned_ids:
        uLogging.info("Cleaning table '%s' versus parent table '%s' by pair %s.%s -> %s.%s",
                      table, parent_table, table, column, parent_table, parent_column)
        s_query = "SELECT * FROM %s WHERE %s IN (%s)" % (table, column, ('%s,' * len(orphaned_ids)).rstrip(','))
        cur = con.cursor()
        cur.execute(s_query, orphaned_ids)
        records = cur.fetchall()
        d_cur = con.cursor()
        d_query = "DELETE FROM %s WHERE %s IN (%s)" % (table, column, ('%s,' * len(orphaned_ids)).rstrip(','))
        d_cur.execute(d_query, orphaned_ids)
        con.commit()
        uLogging.info("Orphaned records has been removed from '%s' where '%s' in (%s) : " %
                      (table, column, ', '.join(map(lambda orphaned_id: str(orphaned_id), orphaned_ids))))
        for record in records:
            uLogging.info("%s", record)


class DBValidator:

    def __init__(self, skip_constraint_names = False):
        self.skip_constraint_names = skip_constraint_names

    def searchRedundantTables(self, schemas, tables):
        """
        search for redundant tables
        :param schemas: table name -> uDBSchema.Table object mapping from manifests
        :param tables: table name -> uDBSchema.Table object mapping as in DB
        :return: list of errors with tables existing in DB and not described in manifests
        """
        errors = []
        for table_name in tables:
            if not table_name in schemas:
                errors.append(DBErrorRedundantTable(tables[table_name]))
        return errors

    def searchMissingTables(self, schemas, tables):
        """
        search for missing tables
        :param schemas: table name -> uDBSchema.Table object mapping from manifests
        :param tables: table name -> uDBSchema.Table object mapping as in DB
        :return: list of errors with tables missing in DB and described in manifests
        """
        errors = []
        for schema_name in schemas:
            if not schema_name in tables:
                errors.append(DBErrorMissingTable(schemas[schema_name]))
        return errors

    def validateTables(self, schemas, tables):
        """
        validate columns, indexes, and constraints in tables according to manifests
        :param schemas: table name -> uDBSchema.Table object mapping from manifests
        :param tables: table name -> uDBSchema.Table object mapping as in DB
        :return: list of errors in tables: wrong columns, indexes, and constraints
        """
        errors = []
        for schema_name in schemas:
            if schema_name == "domains":
                continue
            if schema_name in tables:
                errors += self._validateColumns(schemas[schema_name], tables[schema_name])
                errors += self._validateIndexes(schemas[schema_name], tables[schema_name])
                errors += self._validateConstraints(schemas[schema_name], tables[schema_name])
        return errors

    def repairTables(self, con, schemas, tables):
        """
        repair tables by restoring missing constraints
        :param con: open database connection
        :param schemas: table name -> uDBSchema.Table object mapping from manifests
        :param tables: table name -> uDBSchema.Table object mapping as in DB
        :return: list of errors
        """
        errors = []
        for schema_name in schemas:
            if schema_name in tables:
                errors += self._repairConstraints(con, schemas[schema_name], tables[schema_name], tables)

        return errors

    def _validateColumns(self, schema, table):
        errors = []

        table_columns = {}
        for column in table.columns:
            table_columns[column.name] = column

        schema_columns = {}
        for schema_column in schema.columns:
            schema_columns[schema_column.name] = schema_column
            if not schema_column.name in table_columns:
                errors.append(DBErrorMissingColumn(table, schema_column))
            else:
                table_column = table_columns[schema_column.name]

                if not isinstance(table_column.type, type(schema_column.type)):
                    errors.append(DBErrorNonconformingColumnType(table, table_column, schema_column))
                if getattr(table_column.type, 'size', None) != getattr(schema_column.type, 'size', None):
                    errors.append(DBErrorNonconformingColumnType(table, table_column, schema_column))
                if table_column.nullable and not schema_column.nullable:
                    errors.append(DBErrorColumnMissingNotNull(table, table_column))
                if not table_column.nullable and schema_column.nullable:
                    errors.append(DBErrorColumnRedundantNotNull(table, table_column))

                if table_column.default is not None or schema_column.default is not None:
                    if isinstance(table_column.type, uDBTypes.SerialType):
                        pass  # serial type already checked
                    elif table_column.default is None:
                        errors.append(DBErrorColumnMissingDefault(table, table_column, schema_column.default))
                    elif schema_column.default is None:
                        errors.append(DBErrorColumnRedundantDefault(table, table_column))
                    else:
                        if not isinstance(table_column.default, uDBTypes.DefSysDate):
                            if not uDBSchema.exprsEq(table_column.default, schema_column.default):
                                errors.append(
                                    DBErrorColumnNonconformingDefault(table, table_column, schema_column.default))
                        elif not isinstance(schema_column.default, uDBTypes.DefSysDate):
                            errors.append(DBErrorColumnNonconformingDefault(table, table_column, schema_column.default))

        for table_column in table.columns:
            if not table_column.name in schema_columns:
                errors.append(DBErrorRedundantColumn(table, table_column))
        return errors

    def _validate(self, tablename, schema, table, get_key_raw, fields, DBErrorRedundant, DBErrorMissing, DBErrorNonconforming):
        errors = []
        if not self.skip_constraint_names:
            get_key = lambda x: (x.name, get_key_raw(x))
#            fields.append('name')
        else:
            get_key = get_key_raw

        schema_essences = {}
        for schema_essence in schema:
            schema_essences[get_key(schema_essence)] = schema_essence

        table_essences = {}
        for table_essence in table:
            table_essences[get_key(table_essence)] = table_essence
            if not get_key(table_essence) in schema_essences:
                errors.append(DBErrorRedundant(tablename, table_essence))

        for schema_essence in schema:
            if not get_key(schema_essence) in table_essences:
                errors.append(DBErrorMissing(tablename, schema_essence))
            else:
                table_essence = table_essences[get_key(schema_essence)]
                for f in fields:
                    if getattr(table_essence, f) != getattr(schema_essence, f):
                        errors.append(DBErrorNonconforming(tablename, table_essence, schema_essence))
        return errors

    def _validateIndexes(self, schema, table):
        return self._validate(table, schema.indexes, table.indexes, lambda x: tuple(i.split(' ', 1)[0] for i in x.columns), ['unique'], DBErrorRedundantIndex, DBErrorMissingIndex, DBErrorNonconformingIndex)

    def _validateConstraints(self, schema, table):
        errors = self._validate(table, [x for x in schema.constraints if isinstance(x, uDBSchema.ForeignKey)], [x for x in table.constraints if isinstance(x, uDBSchema.ForeignKey)],
                            lambda constraint: (constraint.reftable,) + tuple(map(lambda x, y: (x, y), constraint.columns[0], constraint.columns[1])),#.sort(lambda x, y: cmp(x[0], y[0])),
                            ['ondelete', 'onupdate'], DBErrorRedundantForeignKey, DBErrorMissingForeignKey, DBErrorNonconformingForeignKey)

        schema_pk = None
        table_pk = None

        schema_constraints = {}
        for constraint in schema.constraints:
            schema_constraints[constraint.name] = constraint

        table_constraints = {}
        for constraint in table.constraints:
            table_constraints[constraint.name] = constraint
            if isinstance(constraint, uDBSchema.PrimaryKey):
                table_pk = constraint
            else:
                if not constraint.name in schema_constraints:
                    if isinstance(constraint, uDBSchema.UniqueKey):
                        errors.append(DBErrorRedundantUnique(table, constraint))
# TMP DIRTY FIX TILL ACC ID=1 INIT IN PAU
#					elif isinstance(constraint, uDBSchema.ForeignKey):
#						errors.append(DBErrorRedundantForeignKey(table, constraint))
                    elif isinstance(constraint, uDBSchema.Check):
                        errors.append(DBErrorRedundantCheck(table, constraint))

        for constraint in schema.constraints:
            if isinstance(constraint, uDBSchema.PrimaryKey):
                schema_pk = constraint
            else:
                if not constraint.name in table_constraints:
                    if isinstance(constraint, uDBSchema.UniqueKey):
                        errors.append(DBErrorMissingUnique(table, constraint))
                    elif isinstance(constraint, uDBSchema.Check):
                        errors.append(DBErrorMissingCheck(table, constraint))
                else:
                    table_constraint = table_constraints[constraint.name]

                    if isinstance(constraint, uDBSchema.UniqueKey):
                        if len(table_constraint.columns) != len(constraint.columns) or table_constraint.columns.sort() != constraint.columns.sort():
                            errors.append(DBErrorNonconformingUnique(table, table_constraint, constraint))
                    elif isinstance(constraint, uDBSchema.Check):
                        if not uDBSchema.exprsEq(table_constraint.expression, constraint.expression):
                            errors.append(DBErrorNonconformingCheck(table, table_constraint, constraint))

        if table_pk is not None or schema_pk is not None:
            if table_pk is None:
                errors.append(DBErrorMissingPrimaryKey(table, schema_pk))
            elif schema_pk is None:
                errors.append(DBErrorRedundantPrimaryKey(table, table_pk))
            elif table_pk.name != schema_pk.name and not self.skip_constraint_names:
                errors.append(DBErrorNonconformingPrimaryKey(table, table_pk, schema_pk))
            elif len(table_pk.columns) != len(schema_pk.columns) or table_pk.columns.sort() != schema_pk.columns.sort():
                errors.append(DBErrorNonconformingPrimaryKey(table, table_pk, schema_pk))
        return errors

    def _repairConstraints(self, con, schema, table, tables):
        """
        restoring missing constraints, that can be restored
        """
        uLogging.info("modified repairConstraints working")
        if table.name == "domains":
            return []

        schema_constraints = {}
        for constraint in schema.constraints:
            schema_constraints[constraint.name] = constraint

        table_constraints = {}
        for constraint in table.constraints:
            table_constraints[constraint.name] = constraint

        constraints_to_restore = []
        for constraint in schema.constraints:
            if not constraint.name in table_constraints:
                if isinstance(constraint, uDBSchema.PrimaryKey):
                    # skip primary key constraints, as things that cannot be missing
                    pass
                elif isinstance(constraint, uDBSchema.Check):
                    # skip check constraints, as it's difficult to identify if it's new and on what column
                    pass
                elif isinstance(constraint, uDBSchema.ForeignKey):
                    if constraint.reftable in tables:
                        constraints_to_restore.append(constraint)
                else:
                    constraints_to_restore.append(constraint)
            else:
                # restore only missing constraints for now, do not check if their description matches one from manifest
                pass

        errors = []
        # try to restore everyone, that was found
        for constraint in constraints_to_restore:
            table_columns = set([unicode(c.name) for c in table.columns])
            if isinstance(constraint, uDBSchema.ForeignKey):
                source_columns_to_constraint = set([c for c in constraint.columns[0]])
                ref_columns_to_constraint = set([c for c in constraint.columns[1]])

                ref_table = tables[constraint.reftable]
                ref_table_columns = set([unicode(c.name) for c in ref_table.columns])

                restore_it = source_columns_to_constraint.issubset(
                    table_columns) and ref_columns_to_constraint.issubset(ref_table_columns)
            else:
                columns_to_constraint = set([c for c in constraint.columns])
                restore_it = columns_to_constraint.issubset(table_columns)

            if restore_it:
                uLogging.info("Adding constraint '%s' to table '%s'", constraint.name, table.name)
                try:
                    table.addConstraint(constraint, con)
                    # fix each successful change
                    con.commit()
                except Exception, e:
                    errors.append("Failed to add constraint '%s' on table '%s': %s" % (constraint.name, table.name, e))
        return errors
