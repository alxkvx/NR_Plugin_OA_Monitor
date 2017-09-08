import os
import sys
import re
import platform

import uUtil
import uLogging
import uPackaging
import uOSCommon
import uBuild
import subprocess
from uAction import *
from uConst import Const

def checkPythonVersion(python_version):
    if python_version < ('2', '7', '6'):
        raise Exception("You must have python version 2.7.6 to install PA on win32")


def determinePlatform():
    pver = platform.version().split('.')
    pVerShort = ".".join(pver[:2])
    p = uBuild.Platform(Const.getOsaWinPlatform(), pVerShort, "x86_64")  # POA uses 2-numbered windows version like 5.2
    p.osverfull = pver		# leave scalar version in osver for compatibility with 5.5
    return p


def check_platform_supported(machine):
    raise Exception("Windows platform is not supported")


def checkUserPrivileges():
    if os.environ['USERNAME'] not in ("Administrator", "BVT-WINMN$"):
        raise Exception("You must be Administrator to install PA")


def checkDiskSpace():
    def getFreeDiskSpace(path):
        import ctypes
        _, total, free = ctypes.c_ulonglong(), ctypes.c_ulonglong(), ctypes.c_ulonglong()
        fun = ctypes.windll.kernel32.GetDiskFreeSpaceExA
        ret = fun(path, ctypes.byref(_), ctypes.byref(total), ctypes.byref(free))
        if ret == 0:
            raise ctypes.WinError()
        return free.value / (1024 * 1024 * 1024)

    path = os.environ['SYSTEMDRIVE']
    uOSCommon.checkDiskSpace(path, 10, getFreeDiskSpace(path))


def getOSUpdates():
    return []


def checkOSUpdates():
    pass


def installJDK(win_files_dir):
    fullPath = os.path.join(win_files_dir, "jdk-8u112.exe")
    runCmd = 'cmd.exe /c start "title" /wait "' + fullPath + '" /s ADDLOCAL="ToolsFeature"'
    uUtil.execCommand(runCmd)


def installVCRedistX64(win_files_dir):
    installWinFile(win_files_dir, "vcredist-2012-x64-11.0.61030.0.exe")


def installWinFile(win_files_dir, file):
    uUtil.execCommand([os.path.join(win_files_dir, file), "/Q"])


def installPrerequisites(source_dir):
    win_files_dir = os.path.join(source_dir, "os", Const.getDistribWinDir())
    installJDK(win_files_dir)
    installVCRedistX64(win_files_dir)
    installWinFile(win_files_dir, "vcredist-2005-x86-8.0.50727.6195.exe")
    installWinFile(win_files_dir, "sql-native-client-driver-x86-10.0.exe")
    installWinFile(win_files_dir, "sql-native-client-driver-x64-10.0.exe")


def getPOAProductId():
    # POA Product ID changes with every poa-core.msi content modification
    # return None if POA not installed
    msiProductName = "Parallels Operations Automation"
    psCmd = "Get-ChildItem HKLM:\\SOFTWARE\\Wow6432Node\\Microsoft\\Windows\\CurrentVersion\\Uninstall " \
            "| ForEach-Object { Get-ItemProperty $_.PSPath } | Where-Object {$_.DisplayName -eq \"%s\"} " \
            "| Select -ExpandProperty PSChildName" % msiProductName
    productId = uUtil.readCmd(["powershell", "-Command", psCmd])
    productId = productId.strip()
    if not productId:
        productId = None
    return productId


def isPOACoreBinariesInstalled():
    productId = getPOAProductId()
    return productId != None


def installPOACoreBinaries(config, progress, build_rpms):
    """
    build_rpms needed just for keep the call signature
    build_rpms used in uLinux, where it's a list of new rpm's to install
    """
    win_files_dir = os.path.abspath(os.path.join(config.source_dir, "os", Const.getDistribWinDir()))
    progress.set_progress(75, "Installing POA MSI")
    logfile = os.environ['SYSTEMDRIVE'] + "\\poacore-install.log"
    uUtil.execCommand(["msiexec", "/i", os.path.join(win_files_dir, "poa-core.msi"), "/l*v",
                       logfile, "POA_INSTALL=1", "FULL_COMPUTER_NAME=" + config.hostname])


def removePOACoreBinaries():
    productId = getPOAProductId()
    uUtil.execCommand("cmd.exe /c start /wait msiexec /x %s /qn" % productId)


def initDefaultProps(config):
    config.sysuser = 'sysuser'		# more convenient variable checking
    config.sysgroup = 'sysgroup'
    config.admin_db_user = "sa"
    config.database_odbc_driver = "SQL Server Native Client 10.0"


def getDefaultRootpath():
    program_files = os.getenv('ProgramFiles(x86)')
    if not program_files:
        program_files = os.environ['ProgramFiles']
    return program_files + '\\SWSoft\\PEM'


def get_default_log_file():
    return os.environ['SYSTEMDRIVE'] + '\\poa-install.log'


def getDefaultMigrateLogFile():
    pass


def getRootpath():
    from _winreg import ConnectRegistry, OpenKey, EnumKey, QueryValueEx, HKEY_LOCAL_MACHINE
    lm = ConnectRegistry(None, HKEY_LOCAL_MACHINE)
    pemKey = OpenKey(lm, r"SOFTWARE\SWSoft\PEM")
    rootpath = None
    i = 0
    while True:		# traverse all keys as hostname may vary
        try:
            hostKeyName = EnumKey(pemKey, i)
            i = i + 1
        except WindowsError:
            break
        try:
            pemHostKey = OpenKey(pemKey, hostKeyName)
            val = QueryValueEx(pemHostKey, "conf_files_path")
            rootpath = os.path.dirname(val[0])
        except WindowsError:
            continue
    return rootpath


def listNetifaces():
    adapterLinePattern = re.compile(r".* adapter (.*):")
    ipLinePattern = re.compile(r"\s*IPv4 Address.* : (.*)")
    maskLinePattern = re.compile(r"\s*Subnet Mask.* : (.*)")
    ipAndMaskPattern = re.compile(r"([0-9]{1,3}\.){3}[0-9]{1,3}")

    ifcfg_out = uUtil.readCmd("ipconfig")

    iface_name = ip = mask = None
    rv = []
    for line in ifcfg_out.splitlines():
        line = line.strip()
        if not line:
            continue
        m = adapterLinePattern.match(line)
        if m:
            iface_name = m.group(1)
            continue
        if iface_name:
            m = ipLinePattern.match(line)
            if m:
                ip = m.group(1)
                if not ipAndMaskPattern.match(ip):
                    raise Exception("ip address %s for adapter %s does not match pattern" % (ip, iface_name))
            m = maskLinePattern.match(line)
            if m:
                mask = m.group(1)
                if not ipAndMaskPattern.match(mask):
                    raise Exception("network mask %s for adapter %s does not match pattern" % (mask, iface_name))
            if ip and mask:
                rv.append((iface_name, ip, mask))
                iface_name = ip = mask = None
    return rv


def createDatabaseAndUser(odbc_driver_name, servername, admin_user, admin_password, dbname, usename, passwd):
    connect_string = "DRIVER={%s}; SERVER=%s; DATABASE=master; UID=%s; PWD=%s" % (
        odbc_driver_name, servername, admin_user, admin_password)
    print "connecting to odbc by %s" % connect_string		# TODO change to uLoggin.debug after installer unification
    import pyodbc
    con = pyodbc.connect(connect_string, autocommit=True)
    print "recreating db %s" % dbname
    cur = con.cursor()
    try:
        cur.execute("DROP database %s" % dbname)
    except Exception, e:
        print e
    except:
        pass

    try:
        cur.execute("EXEC sp_droplogin %s" % usename)
    except Exception, e:
        print e
    except:
        print "Failed to drop", usename

    cur.execute("EXEC sp_addlogin @loginame='%s', @passwd='%s'" % (usename, passwd))
    cur.execute("CREATE DATABASE %s COLLATE Latin1_General_CS_AS" % dbname)
    cur.execute("ALTER DATABASE [%s] SET READ_COMMITTED_SNAPSHOT ON" % dbname)
    cur.execute("ALTER DATABASE %s SET ARITHABORT ON" % dbname)
    cur.execute(" EXEC [%s]..sp_changedbowner '%s'" % (dbname, usename))
    cur.execute("EXEC sp_defaultdb @loginame = '%s', @defdb = '%s'" % (usename, dbname))
    cur.execute("CREATE TABLE [%s]..dual (dummy char(1))" % dbname)
    cur.execute("INSERT INTO [%s]..dual (dummy) VALUES ('X')" % dbname)
    con.commit()
    uSysDB.close(con)


def initSysLog(config):
    print "Creating syslog configs"
    rootpath = config.rootpath
    syslog_etc = os.path.join(rootpath, 'syslog', 'etc')
    syslog_conf = os.path.join(syslog_etc, 'syslog.conf')
    syslog_dir = os.path.join(rootpath, 'var', 'log')
    try:
        os.makedirs(syslog_etc)
    except:
        pass  # probably already exists.
    f = open(syslog_conf, 'w')
    f.write("""<?xml version="1.0"?>
<conf>
	<source name="src_udp" type="udp"/>
	<destination name="poa_log"   file="core.log"       rotate="daily" backlogs="28" dateext="yes" autocompress="yes" size="1700M"/>
	<filter name="poa_log">
		<facility name="local0" />
		<priority name="emerg" />
		<priority name="alert" />
		<priority name="crit" />
		<priority name="error" />
		<priority name="warning" />
		<priority name="notice" />
		<priority name="info" />
		<priority name="debug"/>
	</filter>
	<logpath source="src_udp" filter="poa_log" destination="poa_log" />
	<options logdir="%(syslog_dir)s/pa" />
</conf>
""" % locals())
    f.close()

site_name = "Initial POA tarballs storage"


def createPPMSite(config):
    progress.do("creating ppm site")
    system_root = os.environ['SystemRoot']
    appcmd = os.path.join(system_root, 'System32', 'inetsrv', 'appcmd.exe')
    source_dir = config.source_dir
    tarball_dir = os.path.join(config.rootpath.replace('/', '\\'), 'install', 'tarballs')
    if not os.path.exists(appcmd):
        uLogging.debug("IIS 6")
        out, err, status = uUtil.readCmdExt(
            [os.path.join(source_dir, 'os', Const.getDistribWinDir(), 'IISAdministrationTools', '_install.bat')])
        uLogging.debug("%s %s %s", out, err, status)
        iis_web = os.path.join(system_root, 'System32', 'iisweb.vbs')
        out, err, status = uUtil.readCmdExt(['cscript', iis_web, '/delete', site_name])
        uLogging.debug("%s %s %s", out, err, status)
        out, err, status = uUtil.readCmdExt(
            ['cscript', iis_web, '/create', tarball_dir, site_name, '/i', config.communication_ip])
        uLogging.debug("%s %s %s", out, err, status)
        out, err, status = uUtil.readCmdExt(
            ['cscript', os.path.join(system_root, 'System32', 'iisvdir.vbs'), '/create', site_name, 'tarballs',  tarball_dir])
        uLogging.debug("%s %s %s", out, err, status)
    else:
        uLogging.debug("IIS 7")
        out, err, status = uUtil.readCmdExt('%s delete site /site.name:"%s"' % (appcmd, site_name))
        uLogging.debug("%s %s %s", out, err, status)
        out, err, status = uUtil.readCmdExt(
            '%s add site "/name:%s" /bindings:"http://%s:80" "/physicalPath:%s"' % (appcmd, site_name, config.communication_ip, tarball_dir))
        uLogging.debug("%s %s %s", out, err, status)
        out, err, status = uUtil.readCmdExt(
            '%s add vdir "/app.name:%s/" "/physicalPath:%s" /path:/tarballs' % (appcmd, site_name, tarball_dir))
        uLogging.debug("%s %s %s", out, err, status)

    try:
        os.makedirs(tarball_dir)
    except:
        pass
    out, err, status = uUtil.readCmdExt(
        ["cscript", os.path.join(config.rootpath, 'install', 'tarball_storage_config.vbs'), site_name])
    uLogging.debug("%s %s %s", out, err, status)
    progress.done()

    progress.do("copying tarballs")
    os.path.walk(source_dir, uPackaging.copy_tarballs, tarball_dir)
    progress.done()


def createTMLOGSSite(rootpath, ip):
    progress.do("creating tmlogs site")
    appcmd = os.path.join(os.environ['SystemRoot'], 'System32', 'inetsrv', 'appcmd.exe')
    tdir = os.path.join(rootpath.replace('/', '\\'), 'var', 'taskLogs')
    if os.path.exists(appcmd):
        out, err, status = uUtil.readCmdExt(
            '%s add vdir "/app.name:%s/" "/physicalPath:%s" /path:/tmlogs' % (appcmd, site_name, tdir))
        uLogging.debug("%s %s %s", out, err, status)
        out, err, status = uUtil.readCmdExt(
            '%s set config /section:staticContent /+\"[fileExtension=\'.\',mimeType=\'text/plain\']\"' % (appcmd))
        uLogging.debug("%s %s %s", out, err, status)
    else:
        uLogging.info("IIS 7 appcmd not found, skipping tmlogs site setup")
    try:
        os.makedirs(tdir)
    except:
        pass
    progress.done()

# install service. configure syslogd


def configureSystem(config, progress):
    initSysLog(config)

    def setupService(name, binaryPath, displayName, description):
        nameQuoted = "\"%s\"" % name
        uUtil.execCommand("net stop %s" % nameQuoted, [0, 2])			# service name invalid
        uLogging.info("service %s stopped" % name)
        uUtil.execCommand("sc delete %s" % nameQuoted, [0, 1060])		# service not exist
        uLogging.info("service %s deleted" % name)

        uUtil.execCommand("sc create %s binPath= \"%s\" DisplayName= \"%s\" start= auto" %
                          (nameQuoted, binaryPath, displayName))
        uUtil.execCommand("sc description %s %s" % (nameQuoted, description))
        uLogging.info("service %s created" % name)


    setupService("pleskd", os.path.join(config.rootpath, "bin", "watchdog.exe"),
                 "PEM", "\"Operations Automation service\"")

    syslogdName = "PEM syslogd"
    setupService(syslogdName, os.path.join(config.rootpath, "syslog", "syslogd.exe") + " --service",
                 syslogdName, "\"Operations Automation Syslog Service\"")
    uUtil.execCommand("net start \"%s\"" % syslogdName)
    uLogging.info("service %s started" % syslogdName)

    return


def configureDatabase(props, progress):
    progress.set_progress(24, "Configuring MS SQL")


def configureODBC(config):
    c = """odbcconf /a {CONFIGSYSDSN "%s" "DSN=%s|Database=%s|Server=%s|Description=Primary POA DSN"}""" % (
        config.database_odbc_driver, config.dsn, config.database_name, config.database_host)
    uLogging.debug("configuring ODBC with: %s" % c)
    uUtil.execCommand(c)


def create_db(config, askYesNo):
    p = subprocess.Popen("net stop PAU")
    p.communicate()
    createDatabaseAndUser(config.database_odbc_driver, config.database_host, config.admin_db_user,
                          config.admin_db_password, config.database_name, config.dsn_login, config.dsn_passwd)


def checkUncompatibleJDK():
    pass


def checkTmpDirectory():
    pass

