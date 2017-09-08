import os, cStringIO

import uLogging
import uUtil

odbcinst_ini_template = """[PostgreSQL]
Description     = PostgreSQL driver
Driver          = %(driver)s
Setup           = %(setup)s
FileUsage       = 1
Threading       = 0
"""

odbc_ini_template = """[%(dsn)s]
Description     = PostgreSQL
Driver          = PostgreSQL
Database        = %(database_name)s
Servername      = %(database_host)s
ReadOnly        = No
Port            = %(database_port)s
Protocol        = TCP/IP
ByteaAsLongVarBinary    = 1
"""

postgresql_conf_tail_template = """
listen_addresses = '*' # POA installer
bytea_output = 'escape' # POA installer
"""

pg_hba_conf_tail_template = """
# PLEASE, DO NOT DELETE OR MODIFY THE LINE ABOVE
#
host       all      all         %(ip_address)s     255.255.255.255      %(method)s
#
# DO NOT DELETE OR MODIFY THE LINE UNDER
"""

TargetPostgreSQLVersion = "9.6" #default required PostgreSQL server version!
SupportedPostgreSQLVersions = ("9.0", "9.1", "9.2", "9.3", "9.4", "9.5", "9.6")


PostgreSQLShowCmd = "su - postgres -c 'psql -t -P format=unaligned -c \"SHOW %s\" 2> /dev/null'"


def getPostgreSQLRunningVersionViaShow(run):
    try:
        return run(PostgreSQLShowCmd % "server_version").strip().splitlines()[0]
    except Exception:
        pass
    return None


def getPostgreSQLRunningVersionViaRPM(run):
    try:
        _1stLine = lambda x: x.strip().splitlines()[0]
        postmaster_pid = _1stLine(run("pgrep -f '/postmaster\\b'"))
        postmaster_exe = _1stLine(run("readlink /proc/"+postmaster_pid+"/exe"))
        postmaster_rpm  = _1stLine(run("rpm -qf " + postmaster_exe))
        return _1stLine(run("rpm -q --queryformat '%{version}' "+postmaster_rpm))
    except Exception:
        pass
    return None


class PostgreSQLConfig(object):

    def __init__(self, **kwargs):
        if ("commander" in kwargs) and not (kwargs["commander"] is None):
            self.__run = kwargs["commander"]
            if not hasattr(self.__run, '__call__'):
                raise Exception("Command spec specified is not a callable object")
        else:
            self.__run = lambda x: uUtil.runLocalCmd(x)

        _1stLine = lambda x: x.strip().splitlines()[0]
        show_cmd = None
        if "version" in kwargs:
            self.__ver = kwargs["version"]
            if not (self.__ver in SupportedPostgreSQLVersions):
                raise Exception("Unsupported postgresql version specified %s" % (str(self.__ver),))
            ver = getPostgreSQLRunningVersionViaShow(self.__run)
            if ver and self.__ver == ".".join(ver.split(".")[:2]):
                show_cmd = PostgreSQLShowCmd
        else:
            ver = getPostgreSQLRunningVersionViaShow(self.__run)
            if ver is None:
                ver = getPostgreSQLRunningVersionViaRPM(self.__run)
            else:
                show_cmd = PostgreSQLShowCmd
            if ver is None:
                out = _1stLine(self.__run("psql --version"))
                for v in SupportedPostgreSQLVersions:
                   if v in out:
                       ver = v
                       break
                if ver is None:
                    raise Exception("Found postgresql version " + out + " is not supported.")
            else:
                ver = ".".join(ver.split(".")[:2])
            self.__ver = ver
        try:
            ver = _1stLine(self.__run("rpm -q --queryformat '%{version}' postgresql-server"))
            self.__verLess = self.__ver == ".".join(ver.split(".")[:2])
        except:
            self.__verLess = False
        
        if show_cmd:
            self.__dataDir = _1stLine(self.__run(show_cmd % "data_directory"))
            self.__hbaFile = _1stLine(self.__run(show_cmd % "hba_file"))
            self.__cfgFile = _1stLine(self.__run(show_cmd % "config_file"))
        else:
            if self.__verLess:
                self.__dataDir = "/var/lib/pgsql/data"
            else:
                self.__dataDir = "/var/lib/pgsql/"+self.__ver+"/data"
            self.__hbaFile = self.__dataDir + "/pg_hba.conf"
            self.__cfgFile = self.__dataDir + "/postgresql.conf"

        if self.__verLess:
            self.__binDir = _1stLine(self.__run("rpm -ql postgresql-server | grep /postmaster$"))
        else:
            self.__binDir = _1stLine(self.__run("rpm -ql postgresql%s-server | grep /postmaster$" % ("".join(self.__ver.split(".")),)))
        self.__binDir = os.path.dirname(self.__binDir)

        try:
            if self.__verLess:
                self.__odbcSo = _1stLine(self.__run("rpm -ql postgresql-odbc | grep /psqlodbc\\.so$"))
            else:
                self.__odbcSo = _1stLine(self.__run("rpm -ql postgresql90-odbc | grep /psqlodbc\\.so$"))
            self.__odbcSo = os.path.dirname(self.__odbcSo)
        except:
            if self.__ver == "9.1":
                self.__odbcSo = "/usr/pgsql-9.0/lib/psqlodbc.so"
            else:
                self.__odbcSo = "/usr/lib64/psqlodbc.so"

        self.__osVer = _1stLine(self.__run("python -c 'import platform; print (platform.linux_distribution()[1])'"))
        self.__osVer = self.__osVer.split(".")

    def get_odbc_driver(self):
        return self.__odbcSo

    def get_commander(self):
        return self.__run

    def get_version(self):
        return self.__ver

    def get_os_version(self):
        return ".".join(self.__osVer)

    def get_version_as_int(self):
        return int("".join(self.__ver.split(".")))

    def get_data_dir(self):
        return self.__dataDir

    def get_postgresql_conf(self):
        return self.__cfgFile

    def get_pghba_conf(self):
        return self.__hbaFile

    def get_service_name(self):
        if self.__verLess:
            return 'postgresql';
        return 'postgresql-' + self.__ver;

    def get_bin_dir(self):
        return self.__binDir

    def init_db(self):
        if self.__osVer[0] >= "7": #RHEL7 ?
            if self.__verLess:
                setupTool = self.__binDir+"/postgresql-setup"
            else:
                setupTool = self.__binDir+"/postgresql"+"".join(self.__ver.split("."))+"-setup"
            self.__run('PGSETUP_INITDB_OPTIONS="--locale en_US.UTF-8" '+setupTool+' initdb')
        else:
            self.__run("service "+self.get_service_name()+" initdb en_US.UTF-8")

    def set_autostart(self):
        if self.__osVer[0] >= "7": #RHEL7 ?
            self.__run("systemctl enable " + self.get_service_name())
        else:
            self.__run("chkconfig --add " + self.get_service_name())
            self.__run("chkconfig --levels 345 " + self.get_service_name() + " on")

    def cleanup(self):
        self.stop(True)
        self.__run("rm -fr " + self.__dataDir)

    def is_started(self):
        ver = getPostgreSQLRunningVersionViaShow(self.__run)
        if ver is None:
            ver = getPostgreSQLRunningVersionViaRPM(self.__run)
        if ver:
            return self.__ver == ".".join(ver.split(".")[:2])
        return False

    def stop(self, forcedly = False):
        try:
            if self.__osVer[0] >= "7": #RHEL7 ?
                self.__run("systemctl stop "+self.get_service_name())
            else:
                self.__run("service "+self.get_service_name()+" stop")
        except Exception:
            forcedly = True
            pass

        if forcedly:
            try:
                ppid = self.__run("pgrep -f '/postmaster\\b'").strip().splitlines()[0]
                if ppid:
                    self.__run("pkill -9 -P " + ppid)
                    self.__run("kill -9 " + ppid)
            except Exception:
                pass

    def start(self):
        if self.__osVer[0] >= "7": #RHEL7 ?
            self.__run("systemctl start "+self.get_service_name())
        else:
            self.__run("service "+self.get_service_name()+" start")

    def restart(self):
        if self.__osVer[0] >= "7": #RHEL7 ?
            self.__run("systemctl restart "+self.get_service_name())
        else:
            self.__run("service "+self.get_service_name()+" restart")

    def reload(self):
        if self.__osVer[0] >= "7": #RHEL7 ?
            self.__run("systemctl reload "+self.get_service_name())
        else:
            self.__run("service "+self.get_service_name()+" reload")
