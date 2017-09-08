import os
import re
import time
import socket
import base64

import uPackaging
import uFakePackaging

import uLogging
import uSysDB
import uUtil
import uHCL
import uAction
import uBuild
import uPrecheck
import PEMVersion
import uTextRender
import uURLChecker
import uCrypt
from uConst import Const
import uTasks

if Const.isWindows():
    import _winreg  # needed on Windows

import subprocess as sp


def getPemDirectory():
    if not Const.isWindows():
        return "/usr/local/pem"

    def getPOAHostRegistryKey(hostname):
        baseRegPath = None
        for regPath in ["SOFTWARE\\SWsoft\\PEM", "SOFTWARE\\Wow6432Node\\SWsoft\\PEM"]:
            try:
                regkey = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, regPath, 0, _winreg.KEY_READ)
                baseRegPath = regPath
                break
            except WindowsError:
                pass

        if baseRegPath is None:
            raise Exception('System does not have valid Operation Automation installation.')

        foundHosts = []
        i = 0
        try:
            while True:
                foundHosts += [_winreg.EnumKey(regkey, i)]
                i += 1
        except WindowsError:
            pass
        _winreg.CloseKey(regkey)

        if not len(foundHosts):
            raise Exception('System does not have valid Operation Automation installation.')

        if hostname not in foundHosts:
            raise Exception("Detected: system hostname was changed.\nActual hostname is %s.\nExpected hostnames: %s." % (
                hostname, ', '.join(foundHosts)))

        return _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, '%s\\%s' % (baseRegPath, hostname), 0, _winreg.KEY_READ)

    regkey = getPOAHostRegistryKey(socket.getfqdn())
    try:
        pempath = _winreg.QueryValueEx(regkey, "conf_files_path")[0]
        # It returns something like this:
        #  "C:\Program Files\SWsoft\PEM\etc"
        pempath = os.path.split(pempath)[0]
        _winreg.CloseKey(regkey)

        return pempath
    except:
        _winreg.CloseKey(regkey)
        raise Exception("Failed to find PEM installation directory.")


def waitForJBossStarted(root):
    time_to_wait = 120  # Same value as in linux service synchronous script
    from u import bootstrap
    while ('started in' in open(bootstrap.getJBossDir(root) + '\\standalone\\log\\standalone.log').read()) != True and time_to_wait > 0:
        #print("In loop trying to wait for JBoss")
        time.sleep(1)
        time_to_wait -= 1
    if time_to_wait == 0:
        try:
            proclist = sp.check_output('tasklist').splitlines()
            pid = filter(lambda x:'java' in x, proclist)[0].split()[1]
            stack = sp.check_output(os.environ['java_home']+'/bin/jstack -F %s ' % pid)
            uLogging.warn('waitForJBossStarted failed stacktrace below\n'+stack)
        except Exception, e:
            uLogging.warn("Failed to jstack jboss", e)
        raise Exception("Failed to wait for JBoss started")


def startMN(minimal=False):
    resetPIDs()

    platform, root = getMNInfo()
    if Const.isWindows():
        stopMN()
        # To ensure waitForJBossStarted will do correct
        from u import bootstrap
        os.remove(bootstrap.getJBossDir(root) + '\\standalone\\log\\standalone.log')
        uUtil.readCmd(['net', 'start', 'PAU'])
        # Service is stated as "started" before JBoss is actually started, need to wait
        waitForJBossStarted(root)
        uUtil.readCmd(['net', 'start', 'pem'], valid_codes=[0])
    else:
        if minimal:

            env = dict()
            env.update(os.environ)
            pleskd_env = dict(
                LD_LIBRARY_PATH=str(os.path.join(root, "lib") + ":" + "/usr/pgsql-9.5/lib/"),
                SVC_CONF=str(os.path.join(root, "etc", "svc.conf")),
                PLESKD_PROPS=str(os.path.join(root, "etc", "pleskd.props"))
            )
            env.update(pleskd_env)

            progname = os.path.join(root, "sbin", "pa-agent")

            cmd = []
            cmd.append(progname)
            cmd.append("--props-file=" + os.path.join(root, "etc", "pleskd.props"))
            cmd.append("--scs-to-start=chief,ppm")
            # LD_LIBRARY_PATH="/usr/local/pem/lib:/usr/pgsql-9.4/lib/"
            # PATH="${PATH}:/usr/local/pem/bin" /usr/local/pem/sbin/pleskd
            # --props-file /usr/local/pem/etc/pleskd.props --send-signal
            # --log-to-stderr --scs-to-start chief,ppm
            sp.Popen(cmd, env=env)
        else:
            uUtil.execCommand('service pau start', valid_codes=[0, 1])
            uUtil.execCommand('service pa-agent start')


def stopMN(minimal=False):
    if Const.isWindows():
        # ignore 2 error code, because in 5.4 pem cannot be stopped properly
        uUtil.readCmd(['net', 'stop', 'pem'], valid_codes=[0, 2])
        uUtil.readCmd(['net', 'stop', 'PAU'], valid_codes=[0, 1, 2])
    else:
        if not minimal:
            # pem script actually returns 1 and 123 on valid stops
            uUtil.execCommand('service pa-agent stop', [0, 1, 123])
            uUtil.execCommand('service pau stop', [0, 1, 5])

        # In some mysterious cases "service pa-agent stop" doesn't work
        uUtil.readCmdExt(['killall', '-9', 'pa-agent'])
        uUtil.readCmdExt(['killall', '-9', 'SoLoader'])
    resetPIDs()

_installed_scs = []


def is_sc_installed(sc):
    global _installed_scs
    if not _installed_scs:
        con = uSysDB.connect()
        cur = con.cursor()
        cur.execute("SELECT name FROM service_classes")
        _installed_scs = [row[0] for row in cur.fetchall()]

    return (sc in _installed_scs)


def is_started(name):
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM sc_instances si JOIN service_classes sc ON (si.sc_id = sc.sc_id) WHERE sc.name=%s AND si.pid > 0", name)
    return cur.fetchone()


def resetPIDs():
    """
    Set to null PIDs of POA soloaders in order to show that service controllers aren't started
    NOTE: PIDs of pleskd and vzpemagent WILL NOT be changed
    """
    try:
        con = uSysDB.connect()
        cur = con.cursor()

        cur.execute(
            "UPDATE sc_instances SET pid = -1 WHERE sc_id IN (SELECT sc_id FROM service_classes WHERE name NOT IN ('pleskd', 'vzpemagent'))")

        con.commit()
        uSysDB.close(con)
    except Exception, e:
        uLogging.warn("Failed to reset SoLoader PIDs: %s", e)


def getSCProperty(con, scname, propname):
    cur = con.cursor()
    cur.execute(
        "SELECT value FROM v_props v JOIN sc_instances si ON (v.component_id = si.component_id) JOIN service_classes sc ON (si.sc_id = sc.sc_id) WHERE sc.name = %s and v.name=%s", scname, propname)
    pval = cur.fetchone()
    if not pval:
        return None
    else:
        return pval[0]


def _setComponentProperty(component_id, pkg_id, propname, propvalue, description, cname, con, b64key):
    cur = con.cursor()
    cur.execute("SELECT prop_id, protected FROM properties WHERE pkg_id = %s AND name = %s", (pkg_id, propname))

    row = cur.fetchone()

    value = propvalue
    protected = 'n'

    if row:
        prop_id, protected = row
    else:
        if propname == 'db_passwd' or propname == 'dsn_passwd' or propname == 'windows.admin.user.password':
            protected = 'y'
        else:
            protected = 'n'

        cur.execute("INSERT INTO properties (pkg_id, name, default_value, description, protected) VALUES(%s, %s, %s, %s, %s)",
                    (pkg_id, propname, value, description, protected))
        prop_id = uSysDB.get_last_inserted_value(con, "properties")

    if protected == 'y':
        value = uCrypt.encryptData(propvalue, b64key)

    cur.execute("SELECT value_id FROM component_properties WHERE component_id = %s AND prop_id = %s",
                (component_id, prop_id))

    row = cur.fetchone()
    if row:
        cur.execute("UPDATE property_values SET value = %s WHERE value_id = %s", (value, row[0]))
        uLogging.debug("Updating already existent value %d for %s property %s", row[0], cname, propname)
    else:
        uLogging.debug("Adding new value for %s property %s", cname, propname)
        cur.execute("INSERT INTO property_values(component_id, value) VALUES(%s, %s)", (component_id, value))
        value_id = uSysDB.get_last_inserted_value(con, "property_values")
        cur.execute("INSERT INTO component_properties(component_id, value_id, prop_id ) VALUES(%s, %s, %s)",
                    (component_id, value_id, prop_id))


def setComponentProperty(component_id, propname, propvalue, description='', connection=None, b64key=None):
    if not connection:
        con = uSysDB.connect()
        own = True
    else:
        con = connection
        own = False

    cur = con.cursor()
    cur.execute(
        "SELECT c.pkg_id, p.name, p.ctype, c.host_id FROM components c JOIN packages p ON (p.pkg_id = c.pkg_id) WHERE c.component_id = %s", component_id)

    row = cur.fetchone()
    if not row:
        raise Exception("%s: there is no such component installed", component_id)

    pkg_id = row[0]
    name = "[%s-%s on %d]" % (row[2], row[1], row[3])
    _setComponentProperty(component_id, pkg_id, propname, propvalue, description, name, con, b64key)

    if own:
        con.commit()


def setSCProperty(con, scname, propname, propvalue, description=''):
    cur = con.cursor()
    cur.execute(
        "SELECT c.pkg_id, si.component_id FROM sc_instances si JOIN service_classes sc ON (sc.sc_id = si.sc_id) JOIN components c ON (c.component_id = si.component_id) WHERE sc.name=%s", scname)
    rows = cur.fetchall()
    if rows:
        for row in rows:
            pkg_id = row[0]
            component_id = row[1]
            _setComponentProperty(component_id, pkg_id, propname, propvalue, description, scname, con, None)
    else:
        uLogging.info('sc %s is not installed, cannot set property for it', scname)

# 1. Should be used in PRE section only (in other case updater will not update SC binary and location)
# 2. If SC where pointed by IOR from tasks, specific upgrade shall be implemented
# as IOR contains SC name
# 3. IOR in service references dropped as it is changed (it contains SC name)
# 4. is not suitable for pleskd rename


def renameSC(con, old_scname, new_scname):
    cur = con.cursor()
    # drop IOR's they contain SC name
    # we should rename old SC packages to be upgradable
    cur.execute(
        "UPDATE packages SET name = %s WHERE name = %s AND ctype = 'sc'", new_scname, old_scname)
    # we should rename installed SC itself if any
    cur.execute(
        "UPDATE service_classes SET name = %s WHERE name = %s", new_scname, old_scname)
    # we should update dependencies from renamed SC (pointed by name)
    cur.execute(
        "UPDATE dep_packages SET name = %s WHERE name = %s AND ctype = 'sc'", new_scname, old_scname)


def getAllHosts():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "SELECT h.host_id, h.primary_name, h.htype, p.opsys, p.osrel, p.arch, default_rootpath, h.pleskd_id, h.note FROM hosts h JOIN platforms p ON (p.platform_id = h.platform_id) WHERE h.deleting != 1 ORDER BY h.host_id ASC")

    return [uUtil.PEMHost(row[0], row[1], row[2], uBuild.Platform(row[3], row[4], row[5]), row[6], row[7], row[8]) for row in cur.fetchall()]

def getUIHosts():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "SELECT h.host_id, h.primary_name, h.htype, p.opsys, p.osrel, p.arch, default_rootpath, h.pleskd_id, h.note FROM hosts h JOIN platforms p ON (p.platform_id = h.platform_id) JOIN proxies x ON (h.host_id = x.host_id) WHERE h.deleting != 1 ORDER BY h.host_id ASC")

    return [uUtil.PEMHost(row[0], row[1], row[2], uBuild.Platform(row[3], row[4], row[5]), row[6], row[7], row[8]) for row in cur.fetchall()]

def getHost(host_id):
    """
    :param host_id:
    :return: uUtil.PEMHost
    """
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "SELECT h.host_id, h.primary_name, h.htype, p.opsys, p.osrel, p.arch, default_rootpath, h.pleskd_id, h.note FROM hosts h JOIN platforms p ON (p.platform_id = h.platform_id) WHERE host_id = %s", host_id)

    row = cur.fetchone()
    return uUtil.PEMHost(row[0], row[1], row[2], uBuild.Platform(row[3], row[4], row[5]), row[6], row[7], row[8])


def getHostInfo(con, host_id):
    cur = con.cursor()
    cur.execute(
        "SELECT p.opsys, p.osrel, p.arch, h.default_rootpath, p.platform_id FROM platforms p JOIN hosts h ON (h.platform_id = p.platform_id) WHERE h.host_id = %s", host_id)
    row = cur.fetchone()

    if not row:
        raise Exception("Database inconsistency - there is host with id %s!" % host_id)

    platform = uBuild.Platform(row[0], row[1], row[2])
    platform.platform_id = row[4]
    rootpath = row[3]
    cur.close()
    return platform, rootpath

def getHostCommunicationIP(host_id):
    # if htype == 'c', this is not real server, this is H2E WebCluster.
    # const THostType H_H2E = 'e';

    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("select p.value from hosts h, components c, packages pkg, v_props p where h.host_id=c.host_id and c.pkg_id=pkg.pkg_id and p.component_id=c.component_id and h.htype<>'e' and p.name='communication.ip' and pkg.name='pleskd' and c.host_id = %s", host_id)
    row = cur.fetchone()
    if row:
        return row[0]
    return None

def getBrandingHosts():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "SELECT DISTINCT hosts.host_id, hosts.primary_name "
        "FROM brand_proxy_params brand "
        "JOIN subdomain_services website ON (website.subds_id = brand.vh_id) "
        "JOIN domain_services webspace ON (webspace.ds_id = website.ds_id) "
        "JOIN services on (services.service_id = webspace.service_id) "
        "JOIN hosts on (hosts.host_Id = services.host_Id and hosts.deleting != 1) "
        "WHERE hosts.host_id != 1 AND hosts.htype != 'e'")

    rv = set([(x[0], x[1]) for x in cur.fetchall()])
    cur.close()
    uSysDB.close(con)
    return rv


def getNonMNHosts():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT h.host_id, h.primary_name FROM hosts h WHERE h.host_id != 1 AND h.deleting !=1")

    rv = set([(x[0], x[1]) for x in cur.fetchall()])
    cur.close()
    uSysDB.close(con)
    return rv


_MN_info = None


def getMNInfo():
    # cache used by multithread slave updater
    global _MN_info
    if _MN_info is None:
        con = uSysDB.connect()
        _MN_info = getHostInfo(con, 1)

    return _MN_info


def getPlatform(con, platform_id):
    cur = con.cursor()
    cur.execute("SELECT opsys, osrel, arch FROM platforms WHERE platform_id = %s", platform_id)
    row = cur.fetchone()
    if not row:
        return None
    p = uBuild.Platform(row[0], row[1], row[2])
    p.platform_id = platform_id
    return p


def getPlatformLine(con, platform):
    cur = con.cursor()

    platform_id = platform.platform_id
    rv = [platform]
    while platform_id != 1:
        cur.execute("SELECT parent_id FROM platforms WHERE platform_id = %s", platform_id)
        row = cur.fetchone()
        if row is not None:
            platform_id = row[0]
        else:
            break
        p = getPlatform(con, platform_id)
        if p is not None:
            rv.append(p)
        else:
            break

    cur.close()
    return rv


def ping(host_id):
    ip = getHostCommunicationIP(host_id)
    port = 8352
    try:
        if ip:
            uURLChecker.try_connect((ip, port), 2)
    except socket.error, e:
        raise Exception('Connection to host %s failed (%s). Please ensure host is online and pa-agent service is running on it.' % (ip, e))
    hcl = uHCL.Request(host_id=host_id)
    host = getHost(host_id)
    if Const.isOsaWinPlatform(host.platform.os):
        hcl.command("echo 0")
    else:
        hcl.set_creds(user='root')
        hcl.command("/bin/echo 0")
    try:
        hcl.performCompat()
        return
    except uUtil.ExecFailed:
        pass
    execCtl('pleskd_ctl', ['ping', str(host_id)])

def _execCtl(ctlname, fun, *args):
    platform, root = getMNInfo()

    if len(args) == 1 and type(args[0]) in (tuple, list):
        parameters = (args)[0]
    else:
        parameters = args

    command = [os.path.join(root, 'bin', ctlname), '-f', os.path.join(root, 'etc', 'pleskd.props')] + list(parameters)
    return fun(command)


def execCtl(ctlname, *args):
    return _execCtl(ctlname, uUtil.execCommand, *args)


def readCtl(ctlname, *args):
    return _execCtl(ctlname, uUtil.readCmd, *args)


def checkOneHostAvailability(host, report_only=False):
    try:
        uLogging.info("Checking if host %s is available" % host)
        ping(host.host_id)
    except Exception, e:
        msg = "Failed to ping host %s: %s" % (host, e)
        if report_only:
            return msg
        else:
            raise Exception(
                msg + "\nYou can skip this host and upgrade it manually later with 'ignore', or abort the update. ")
    uLogging.debug("ping host %s succeeded.", host)
    return ""


def checkHostsAvailability(in_precheck=False):
    uLogging.info("Checking slave hosts availability...")
    # skip hosts without pleskd (they are not managed by POA)
    hosts = filter(lambda h: h.pleskd_id and int(h.host_id) != 1, getAllHosts())
    results = []
    for host in hosts:
        if in_precheck:
            res = checkOneHostAvailability(host, True)
            results.append((res, host))
        else:
            uAction.retriable(checkOneHostAvailability)(host)

    # need to make aggregated exception in precheck
    if in_precheck:
        not_reachable_hosts = filter(lambda x: x[0], results)
        if not_reachable_hosts:
            message = ""
            for msg, host in not_reachable_hosts:
                message += "\n * %s (%s)	 error message: %s" % (host.name, host.host_id, msg)
            raise uPrecheck.PrecheckFailed(
                "Some Operation Automation slave hosts not available", "check the following hosts, probably unreacheable by network or have pleskd agent down:%s" % message)


def canProceedWithHost(host):
    # will return "" (host available), None (error ignored), or raise exception (host unavailable & abort selected)
    msg = uAction.retriable(checkOneHostAvailability)(host)
    return msg == ""


def checkNumberOfActiveTasks():
    import uTasks
    task_number = uTasks.getNumberOfActiveTasks()
    if task_number > 2000:
        uLogging.warn("Number of active tasks in Task Manager: %s. Too many active tasks.", task_number)
        raise uPrecheck.PrecheckFailed("There are %s unprocessed, scheduled, running or failed tasks in Task Manager (including periodic).\nUpdate cannot be performed if this number is more than 2000" %
                                       task_number, "Cancel failed tasks or wait for them to complete.")
    else:
        uLogging.info("Number of active tasks in Task Manager: %s. OK.", task_number)


def check_unfinished_installation_tasks():
    unfinished_tasks_num = uTasks.get_num_of_unfinished_installation_tasks()

    if not unfinished_tasks_num:
        uLogging.info("No unfinished installation tasks in Task Manager: OK.")
    else:
        msg = "A number of unfinished installation tasks have been fetched out during pre-check, " \
              "total: %s. " \
              "These issues prevent the update of being started." % unfinished_tasks_num
        uLogging.warn(msg)
        raise uPrecheck.PrecheckFailed(msg, "To resolve these issues, log in to Odin Automation, "
                                            "go to Operations > Tasks, filter the tasks by the 'Install' "
                                            "sample in their name, eliminate blocker factors "
                                            "or fix the cause of the tasks failure"
                                            "then wait for these tasks until they will finish "
                                            "with the Successful status."
                                            "Also you can cancel these tasks, but only if you know what you doing.")


def checkRequirements(build_info):
    checkRequirements2(build_info.depends_to_check, build_info.present_updates)

# should be called as update precheck
# required_updates - build names we need as dependencies, set()
# present_updates - build names we have in update run, set()
def checkRequirements2(required_updates, present_updates):
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT build FROM version_history")
    installed = reduce(lambda s, x: s.update([x[0]]) or s, cur.fetchall(), set())
    cur.execute("SELECT name FROM hotfixes")
    installed = reduce(lambda s, x: s.update([x[0]]) or s, cur.fetchall(), installed)

    # Crutch for upgrade from OA 6.5 TODO: Remove in OA 7.1
    if "oa-6.5-258" in installed:
        installed.add("poa-6.0-3517")

    not_installed = required_updates - installed
    if not_installed == set([None]) or not_installed == set([]):
        not_installed = False
    for build_name in present_updates:
        for inst in installed:
            if PEMVersion.compareVersions(inst, build_name) > 0:
                raise uPrecheck.PrecheckFailed(
                    "%s is installed, it is higher version than %s" % (inst, build_name), None)

    if not_installed:
        raise uPrecheck.PrecheckFailed("required Operation Automation versions are not installed: %s" % ', '.join(
            not_installed), "Install missing versions or select another update")

    cur.close()
    uSysDB.close(con)


def getHostsWithPleskdRPM():
    hosts = getAllHosts()
    hosts_to_check = [host for host in hosts if host.type in ('n', 'c', 'f', 'g') and not Const.isOsaWinPlatform(host.platform.os)]
    return hosts_to_check


def checkURLAccessFromServiceNodes(hosts_to_check, url, in_precheck=False, proxy=""):
    problem_hosts = []

    for host in hosts_to_check:
        try:
            proxy_arg = ""
            if proxy:
                proxy_arg = "--proxy %s" % proxy
            request = uHCL.Request(host.host_id, user='root', group='root')
            # HTTP 301 redirect for requesting folders w/o trailing slash
            request.command(
                "curl -o /dev/null --silent --head -L --write-out '%%{http_code}' %s %s" % (url, proxy_arg), stdout='stdout', stderr='stderr', valid_exit_codes=range(0, 100))
            output = request.performCompat()
            if output['stdout'] != "200":
                uLogging.err('URL "%s" is not accessible from host "%s": HTTP response code is "%s"' % (
                    url, host, output['stdout']))
                problem_hosts += [(host, 'HTTP response code for %s is %s' % (url, output['stdout']))]
        except Exception, e:
            uUtil.logLastException()
            problem_hosts += [(host, str(e))]
        except:
            uUtil.logLastException()
            problem_hosts += [(host, 'General error')]

    if problem_hosts:
        table = uTextRender.Table()
        table.setHeader(["host", "url", "issue"])
        for issue in problem_hosts:
            host, error = issue
            table.addRow([str(host), url, error])
        message = "URL %s is expected to be available from all managed hosts with pleskd RPM installed. " \
                  "Some of them listed bellow have access problems:\n%s" % (url, table)
        recommendation = "Fix accessibility problems."
        raise uPrecheck.PrecheckFailed(message, recommendation)


def getPleskdProps():
    dummy, rootpath = getMNInfo()
    propfile = open(os.path.join(rootpath, 'etc', 'pleskd.props'))
    pleskdProps = uUtil.readPropertiesFile(propfile)
    propfile.close()
    return pleskdProps


def get_major_version():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT version FROM version_history ORDER BY install_date DESC")
    return cur.fetchone()[0]


class NotEnoughFreeDiskSpace(Exception):
    def __init__(self, node_name = None, free_space = None, quota_in_gb = None, errorMessage = None):
        if errorMessage:
            self.reason = errorMessage
            Exception.__init__(self, "Precheck error: " + errorMessage)
            return

        self.intro = "Available disk space on node %s is %iGb, " \
                     "required free space is %iGb, " % (node_name, free_space, quota_in_gb)
        self.fin1 = "it's OK"
        self.fin2 = "not enough free disk space."
        self.reason = self.intro + self.fin2
        # Using directly Exception.__init__ call for python 2.4 compatibility
        Exception.__init__(self, "Precheck error: " + self.intro + self.fin2)


def check_free_disk_space(node_id, quota_in_gb):
    """Checking node availability, checking free disk space on any node
    :param node_id:
    :param quota_in_gb:
    :return:
    """
    node_name = getHost(node_id).name
    uLogging.debug("Making request to node %s for checking free disk space with df utilite.", node_name)
    try:
        uLogging.debug("Checking if node %s is available" % node_name)
        ping(node_id)
    except uUtil.ExecFailed, e:
        uLogging.warn("Failed to ping node %s: %s" % (node_name, e))
        return

    request = uHCL.Request(node_id, user='root', group='root')
    request.command("df /usr | awk 'END{print $(NF-2)}'", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
    free_space = int(request.performCompat()['stdout'])/1048576
    fds_exception = NotEnoughFreeDiskSpace(node_name, free_space, quota_in_gb)
    if free_space >= quota_in_gb:
        uLogging.debug(fds_exception.intro + fds_exception.fin1)
    else:
        uLogging.debug(fds_exception.intro + fds_exception.fin2)
        raise fds_exception


def update_packages_on_host(host_id, packages):
    # at first upgrade pleskd, config and Privileges packages, their dependencies cannot be resolved by ppm properly

    packages_first = filter(lambda x: re.match("pleskd|config|Privileges|libsso", x.package.name), packages)
    for pkg in packages_first:
        uAction.progress.do("installing %s (%s)", pkg.package, pkg.pkg_id)
        uAction.retriable(uPackaging.installPackageToHostAPI, allow_console=True)(
            host_id, print_name=str(pkg), pkg_id=pkg.pkg_id)
        uAction.progress.done()

    # update others using meta-package
    packages_ids = [pkg.pkg_id for pkg in packages]
    uAction.retriable(uFakePackaging.installMetaPackage, allow_console=True)(host_id, pkg_id_list=packages_ids)
