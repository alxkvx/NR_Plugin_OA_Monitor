import sys
import exceptions
import time
import re
import copy
import uLogging
from uConst import Const

paramstyle = 'pyformat'
apilevel = "2.0"
threadsafety = "1"
log_query = True

class Warning(exceptions.StandardError):

    def __init__(self, *args):
        exceptions.StandardError.__init__(self, *args)


class Error(exceptions.StandardError):

    def __init__(self, *args):
        exceptions.StandardError.__init__(self, *args)


class DatabaseError(Error):

    def __init__(self, *args):
        Error.__init__(self, *args)


class OperationalError(DatabaseError):

    def __init__(self, *args):
        DatabaseError.__init__(self, *args)


class DeadlockError(OperationalError):

    def __init__(self, *args):
        OperationalError.__init__(self, *args)


class NotSupportedError(DatabaseError):

    def __init__(self, *args):
        DatabaseError.__init__(self, *args)


class Cursor:
    query_fix_patterns = []

    def _update_attrs(self):
        if hasattr(self.cursor, 'description'):
            self.description = self.cursor.description
        else:
            self.description = None

        if hasattr(self.cursor, 'rowcount'):
            self.rowcount = self.cursor.rowcount
        else:
            self.rowcount = None

    def close(self):
        self.cursor.close()
        del self.cursor

    def callproc(self, *args):
        raise NotSupportedError("No stored procedures (%s), please", args)

    def __init__(self, cursor):
        self.cursor = cursor

    def fetchone(self):
        rv = self.cursor.fetchone()
        return rv

    def fetchmany(self, *args):
        rv = self.cursor.fetchmany(*args)
        return rv

    def fetchall(self):
        rv = self.cursor.fetchall()
        return rv

    def nextset(self):
        raise NotSupportedError("Multiple result sets are not supported")

    def setinputsizes(self, *args):
        pass

    def setoutputsize(self, *args):
        pass

    def query_fix(self, statement):
        for p in self.query_fix_patterns:
            statement = re.sub(p[0], p[1], statement)
        return statement


class DictConverter:
    # a bit hackerish. It assumes that % formatting calls []
    # exactly once for each converting. It does so.

    def __init__(self):
        self.argmap = {}
        self.arglist = []

    def convert(self, format, argmap):
        self.arglist = []
        self.argmap = argmap

        return format % self.argmap, tuple(self.arglist)

    def __getitem__(self, key):
        self.arglist.append(self.argmap[key])
        return '?'


class QmarkCursor(Cursor):
    query_fix_patterns = [('MSSQL:', ''), ('PGSQL:.*\n', '\n'), ('`(\w+)`', r'[\1]')]

    def __init__(self, cursor):
        Cursor.__init__(self, cursor)

    def execute_org(self, statement, *args):
        if len(args) == 0:
            rv = self.cursor.execute(statement)
        elif len(args) != 1:
            # parameters is sequence
            rv = self.cursor.execute(statement % (('?',) * len(args)), args)
        elif type(args[0]) == list or type(args[0]) == tuple:
            # parameters are sequence ==> positional substitution
            # just replace %s with ?.
            rv = self.cursor.execute(statement % (('?',) * len(args[0])), args[0])
        elif type(args[0]) == dict:
            rv = self.cursor.execute(*(DictConverter().convert(statement, args[0])))
        else:
            # one parameter
            rv = self.cursor.execute(statement % '?', (args[0],))

        return rv

    def execute(self, statement, *args, **kwargs):
        statement = self.query_fix(statement)
        try:
            return self.execute_org(statement, *args)
        except:
            exctype, excvalue, traceback = sys.exc_info()
            if type(exctype) == str:
                raise DatabaseError(exctype, excvalue, "while executing %s (%s)" % (statement, args))
            raise

    def fetchone(self):
        try:
            rv = self.cursor.fetchone()
            return rv
        except:
            exctype, excvalue, traceback = sys.exc_info()
            if type(exctype) == str:
                raise DatabaseError(exctype, excvalue, "while fetching one row")
            raise


class PyFormatCursor(Cursor):
    query_fix_patterns = [('PGSQL:', ''), ('MSSQL:.*\n', '\n'), ('`(\w+)`', r'"\1"')]

    def __init__(self, cursor):
        Cursor.__init__(self, cursor)

    def execute(self, statement, *args, **kwargs):
        global log_query
        statement = self.query_fix(statement)
        try:
            if log_query:
                copied_args = list(copy.copy(args))
                if 'hide_param_index' in kwargs:
                    pn = kwargs['hide_param_index']
                    copied_args[pn] = '***'
                uLogging.debug("SQL: %s (%s)", statement, tuple(copied_args))

            if not args:
                rv = self.cursor.execute(statement)
            elif len(args) == 1 and type(args[0]) in (tuple, list):
                rv = self.cursor.execute(statement, args[0])
            else:
                rv = self.cursor.execute(statement, args)
        except NativeOperationalError, e:
            o_args = list(e.args)
            e_args = ["executing stmt with params:", statement, args] + o_args
            for a in o_args:
                if a and str(a).startswith("deadlock detected"):
                    raise DeadlockError(e_args)
            raise OperationalError(e_args)
        except Exception, e:
            print e, e.args
            e_args = ["executing stmt with params:", statement, args] + list(e.args)
            raise DatabaseError(e_args)
        return rv

all_connections = {}


class Connection:

    def __init__(self, connection):
        self.connection = connection

    def _close(self):
        conn_alr_cl_pattern = "connection already closed"
        try:
            self.connection.close()
        except Exception as e:
            if conn_alr_cl_pattern in e.message:
                uLogging.debug("The exception \"%s\" "
                               "was raised during closing the connection %s. " % (conn_alr_cl_pattern,
                                                                                  str(self.connection)))
            else:
                raise e
        uLogging.debug("Connection %s was closed" % self.connection)

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        self.connection.rollback()
        uLogging.debug("Transaction was rolled back")

    def insertRecord(self, table, **kwds):
        colstr = ""
        params = []
        for k in kwds:
            if colstr:
                colstr += ', '
            colstr += '`%s`' % k
            params.append(kwds[k])
        cur = self.cursor()
        stmt = "INSERT INTO `%s` (%s) VALUES (%s)" % (table, colstr, ', '.join(['%s'] * len(params)))

        cur.execute(stmt, params)

    def insertRecordWithId(self, table, **kwds):
        self.insertRecord(table, **kwds)
        return get_last_inserted_value(self, table)


class PyFormatConnection(Connection):

    def __init__(self, connection):
        Connection.__init__(self, connection)
        self.cursor().execute('set standard_conforming_strings = off')

    def cursor(self, *args, **kwds):
        import psycopg2.extras
        return PyFormatCursor(self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor, *args, **kwds))

    def commit(self):
        import psycopg2
        try:
            self.connection.commit()
        except psycopg2.Warning, e:
            uLogging.warn("%s", e)


class QmarkConnection(Connection):

    def __init__(self, connection):
        Connection.__init__(self, connection)

    def cursor(self, *args, **kwds):
        return QmarkCursor(self.connection.cursor(*args, **kwds))


def connect_impl(*args, **kwargs):
    raise Error("uSysDB was not properly initialized")


def connect(*args, **kwargs):
    init_default_config_if_none()
    return connect_impl(*args, **kwargs)

_inited = None

DBType = None

PgSQL = 1
MSSQL = 2
ODBC = 3

ConcatOperator = None
nowfun = None


def get_last_inserted_value(con, tablename):
    raise Error("uSysDB was not properly initialized")


def convertDTstring(dummy):
    raise Error("uSysDB was not properly initialized")


def toRaw(dummy):
    raise Error("uSysDB was not properly initialized")

NativeOperationalError = KeyboardInterrupt


def init_default_config_if_none():
    global _config
    if not _config:
        import uConfig
        init(uConfig.Config())


def setup_connection():
    global _inited
    global connect_impl
    global DBType
    global ConcatOperator
    global nowfun
    global _config
    global all_connections
    global get_last_inserted_value
    global toRaw
    global convertDTstring
    global NativeOperationalError

    uLogging.debug('Db connection setup. Database host %s, type %s, name %s, login %s, odbc driver %s' % (
        _config.database_host, _config.database_type, _config.database_name, _config.dsn_login, _config.database_odbc_driver))
    if _config.database_type == 'MSSQL':
        import pyodbc

        def connect_win(super_user=False):
            database_name = _config.database_name
            uname = _config.dsn_login
            pwd = _config.dsn_passwd

            if super_user:
                uname = 'sa'

            if not all_connections.has_key((database_name, uname, pwd)):
                if _config.database_odbc_driver is None:
                    connect_string = "DSN=%s;UID=%s;PWD=%s" % (database_name, uname, pwd)
                else:
                    connect_string = "DRIVER=%s; SERVER=%s; DATABASE=%s; UID=%s; PWD=%s" % (
                        _config.database_odbc_driver, _config.database_host, database_name, uname, pwd)
                all_connections[(database_name, uname, pwd)] = QmarkConnection(pyodbc.connect(connect_string))
            return all_connections[(database_name, uname, pwd)]

        connect_impl = connect_win
        DBType = MSSQL
        ConcatOperator = '+'

        def toRaw_mssql(x):
            if x is None:
                return x
            else:
                return pyodbc.Binary(x)
        toRaw = toRaw_mssql

        def get_last_inserted_value_mssql(con, tablename):
            cur = con.cursor()
            try:
                cur.execute("SELECT @@IDENTITY")
                return cur.fetchone()[0]
            finally:
                cur.close()
        get_last_inserted_value = get_last_inserted_value_mssql
        nowfun = 'getdate'

        convertDTstring = lambda t: time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(t))

    elif _config.database_type == 'PGSQL':
        import psycopg2 as _dbmodule
        NativeOperationalError = _dbmodule.OperationalError

        def toRaw_pgsql(x):
            if x is None:
                return x
            else:
                import psycopg2 as _dbmodule
                return _dbmodule.Binary(x)
        toRaw = toRaw_pgsql
        convertDTstring = lambda x: x

        def connect_unix(super_user=False):
            database_name = _config.database_name

            if super_user:
                uname = 'postgres'
            else:
                uname = _config.dsn_login
            if not all_connections.has_key((database_name, uname)):
                import psycopg2 as _dbmodule
                all_connections[(database_name, uname)] = PyFormatConnection(
                    _dbmodule.connect(host=_config.database_host, user=uname, password=_config.dsn_passwd, database=database_name))

            rv = all_connections[(database_name, uname)]
            try:
                cur = rv.cursor()
                cur.execute("SELECT 1")
            except OperationalError:
                uLogging.warn("Connection to DB is lost, attempting to reconnect")
                import psycopg2 as _dbmodule
                close(rv)
                rv = all_connections[(database_name, uname)] = PyFormatConnection(
                    _dbmodule.connect(host=_config.database_host, user=uname,
                                      password=_config.dsn_passwd, database=database_name))

            return rv

        def get_last_inserted_value_pgsql(con, tablename):
            cur = con.cursor()
            #                       cur.execute("SELECT CURRVAL('%s_seq')" % tablename)
            import uDBSchemaPgSQL
            columns = uDBSchemaPgSQL.get_identity_columns(tablename, con)
            if columns:
                cur.execute("SELECT CURRVAL('`%s`')" % columns[0].sequence)
                return cur.fetchone()[0]
            return None

        connect_impl = connect_unix

        get_last_inserted_value = get_last_inserted_value_pgsql

        DBType = PgSQL
        ConcatOperator = '||'
        nowfun = 'now'

    else:
        raise Error("Failed to initialize uSysDB: Unknown database_name type")

    _inited = True

_config = None


def table_exist(con, table_name):
    global DBType
    if DBType == PgSQL:
        query = "SELECT 1 FROM pg_class WHERE relname = %s"
    else:
        query = "SELECT 1 FROM sys.objects WHERE name = %s"
    cur = con.cursor()

    cur.execute(query, table_name)
    if cur.fetchone():
        return True
    else:
        return False


def rollback_all():
    for i in all_connections:
        all_connections[i].rollback()


def commit_all():
    for i in all_connections:
        all_connections[i].commit()


def disconnect_all():
    for k, v in all_connections.items():
        v._close()
    all_connections.clear()
    uLogging.debug("All DB connections were closed and removed from pool")


def close(connection):
    for k, v in all_connections.items():
        if v == connection:
            v._close()
            del all_connections[k]
    uLogging.debug("Connection %s was removed from pool" % connection.connection)


def init(config=None):
    global _config
    global _inited

    if config is not None:
        _config = config
    if _config is None:
        raise Exception, "config arg is mandatory when you're initializing uSysDB first time"

    if not hasattr(config, "database_type"):
        if Const.isWindows():
            _config.database_type = 'MSSQL'
        else:
            _config.database_type = 'PGSQL'
    else:
        _config.database_type = config.database_type

    if not _inited:
        setup_connection()

    global DBType
    uLogging.debug('Using database_name: %s(%s)' % (_config.database_name, DBType))
    import uDBSchema
    uDBSchema.init(DBType)


def set_verbose(verbose):
    global log_query
    log_query = verbose


class PerThreadConnections:
    import threading

    __tlsObject = threading.local()
    __dbConsLock = threading.Lock()
    __dbConsDict = {}

    @classmethod
    def getConnection(cSelf):
        import thread
        connect = getattr(cSelf.__tlsObject, 'connect', None)
        if connect is None:
            init_default_config_if_none()
            global _config
            try:
                import psycopg2 as _dbm
            except ImportError:
                from pyPgSQL import PgSQL as _dbm
            connectId = thread.get_ident()
            cSelf.__dbConsLock.acquire()
            try:
                nativeConnect = _dbm.connect(host=_config.database_host, user=_config.dsn_login, password=_config.dsn_passwd, database=_config.database_name)
                connect = cSelf.__dbConsDict[connectId] = PyFormatConnection(nativeConnect)
            finally:
                cSelf.__dbConsLock.release()
            cSelf.__tlsObject.connect = connect
        return connect

    @classmethod
    def onError(cSelf, xErr):
        import thread
        cSelf.__tlsObject.connect = xErr
        connectId = thread.get_ident()
        cSelf.__dbConsLock.acquire()
        try:
            if connectId in cSelf.__dbConsDict:
                connect = cSelf.__dbConsDict[connectId]
                if isinstance(connect, Connection):
                    connect.close()
            cSelf.__dbConsDict[connectId] = xErr
        finally:
            cSelf.__dbConsLock.release()

    @classmethod
    def closeAll(cSelf):
        dbSessions = None
        cSelf.__dbConsLock.acquire()
        try:
            dbSessions = list(cSelf.__dbConsDict.values())
        finally:
            cSelf.__dbConsLock.release()
        xErr = None
        for c in dbSessions:
            if not isinstance(c, Connection):
                xErr = c
                break
        if xErr:
            for c in dbSessions:
                if isinstance(c, Connection):
                    c.rollback()
                    c.close()
            raise xErr
        else:
            for c in dbSessions:
                c.commit()
                c.close()

__all__ = ['connect', 'table_exist', 'get_last_inserted_value', 'paramstyle', 'apilevel', 'threadsafety',
           'DBType', 'MSSQL', 'PgSQL', 'toRaw', 'ConcatOperator', 'PerThreadConnections']
