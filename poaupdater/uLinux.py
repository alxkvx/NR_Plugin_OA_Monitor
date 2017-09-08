import os
import re
import socket
import errno
import yum
import grp
import pwd
import platform
import shutil

import uUtil
import uBuild
import uPackaging
import uLogging
import uOSCommon
import uPrecheck
import uCrypt
import uDialog
import uPgSQL
import time


ssl_proxy_vhost_template = """LoadModule ssl_module /usr/lib64/httpd/modules/mod_ssl.so
Listen %(port)s
<VirtualHost *:%(port)s>
        Header always set Strict-Transport-Security "max-age=31536000; includeSubDomains"

        SSLEngine on
        SSLProxyEngine on
        SSLCertificateKeyFile %(key)s
        SSLCertificateFile %(cert)s
        SSLVerifyClient none
        SSLProtocol all -SSLv2 -SSLv3
        SSLCipherSuite HIGH:!ADH:!RC4:!aNULL:!eNULL:!EXP:!MEDIUM:!LOW:!MD5

        #ServerName %(host)s

        ProxyRequests Off

        ProxyPass /bss-static http://%(ip)s:8090/bss-static
        ProxyPassReverse /bss-static http://%(ip)s:8090/bss-static

        ProxyPass /bss-www http://%(ip)s:8090/bss-www
        ProxyPassReverse /bss-www http://%(ip)s:8090/bss-www

        ProxyPass "/ws/" "ws://localhost:8080/ws/" disablereuse=on

        ProxyPass / http://localhost:8080/  timeout=600
        ProxyPassReverse / http://localhost:8080/

        Header edit Location ^http(\:.*)$ https$1
        RequestHeader set X-Forwarded-Proto https

        ProxyPreserveHost On
        KeepAlive On

        ErrorLog /var/log/httpd/ppa-ssl-error.log

</VirtualHost>
"""


admin_db_user = 'postgres'
admin_db = 'template1'

pgsql = None        # uPgSQL.PgSQL91 / uPgSQL.PgSQL92

__platform = None


def checkPythonVersion(python_version):
    if python_version < ('2', '6', '6'):
        raise Exception("You must have python version 2.6.6 or more to install PA on linux")


def determinePlatform():
    global __platform
    if not __platform:
        distname, version, id = platform.linux_distribution()
        __platform = uBuild._create_platform(distname, version, platform.machine())
    return __platform


def check_platform_supported(machine):
    min_os_version = (7, 0)
    os_requirement = "Only x86_64 RedHat/CentOS %d.%d or higher are supported" % min_os_version

    if not machine.os in ["Red Hat Enterprise Linux Server", "RHEL", "CentOS"]:
        raise Exception(("Installation on %s is not possible. " + os_requirement) % machine.os)

    osverfull = tuple(map(lambda v: int(v), machine.osverfull))

    if osverfull < min_os_version:
        osverfull_string = ".".join(machine.osverfull)
        raise Exception(("Current RedHat/CentOS version is %s. " + os_requirement) % osverfull_string)

    if "x86_64" != machine.arch:
        raise Exception(("Current os arch is %s. " + os_requirement) % machine.arch)


def service_control(command, name, valid_codes=None):
    if int(determinePlatform().osver) >= 7:
        c = "systemctl %s %s" % (command, name)
    else:
        c = "service %s %s" % (name, command)
    uUtil.execCommand(c, valid_codes)


def service_autostart(name):
    if int(determinePlatform().osver) >= 7:
        c = "systemctl enable %s" % name
    else:
        c = "chkconfig --add {0}; chkconfig --levels 345 {0} on".format(name)
    uUtil.execCommand(c)


def checkUserPrivileges():
    if os.getuid() != 0:
        raise Exception("You must be superuser to install PA")


def checkDiskSpace():
    def getFreeDiskSpace(path):
        disk_info = os.statvfs(path)
        return disk_info.f_frsize * disk_info.f_bavail / (1024 * 1024 * 1024)
    uOSCommon.checkDiskSpace("/usr/local", 4, getFreeDiskSpace("/usr/local"))
    uOSCommon.checkDiskSpace("/var", 4, getFreeDiskSpace("/var"))


def check_cpu_performance(min_cpu_count=4):

    uLogging.debug("Checking CPU performance via counting CPU cores.")
    uPrecheck.warn_precheck_for_deprecated_os()  # because there is no multiprocessing module in Python 2.4 (RHEL5)
    import multiprocessing
    current_cpu_count = multiprocessing.cpu_count()
    if current_cpu_count < min_cpu_count:
        raise uPrecheck.PrecheckFailed("Your server doesn't meet hardware requirements, "
                                       "at least %i CPU required to install OA, "
                                       "but your server has %i CPU." % (min_cpu_count, current_cpu_count))
    ok_text = "Your CPU performance is OK (required %i CPU, detected %i CPU)." % (min_cpu_count, current_cpu_count)
    uLogging.debug(ok_text)
    return ok_text


def getOSUpdates(PA_YUM_REPO_ID):
    import yum
    listUpdates = []
    try:
        yb = yum.YumBase()
        yb.preconf.init_plugins = False
        yb.preconf.debuglevel = 0
        yb.preconf.errorlevel = 0
        yb.conf.cache = 0
        if PA_YUM_REPO_ID in map(lambda x: x.id, yb.repos.listEnabled()):
            yb.repos.disableRepo(PA_YUM_REPO_ID)

        ypl = yb.doPackageLists('updates')
        listUpdates = ypl.updates
    finally:
        yb.closeRpmDB()

    return listUpdates


def checkOSUpdates(PA_YUM_REPO_ID):
    if len(getOSUpdates(PA_YUM_REPO_ID)) > 0:
        raise Exception("The system is not up to date. Please update system with 'yum update' command.")
    pass


def getPACentralYumRepoBaseURL():
    import yum
    PA_YUM_REPO_ID = 'pa-central-repo'
    pa_repo = None
    yb = yum.YumBase()
    for p in yb.repos.listEnabled():
       if p.id == PA_YUM_REPO_ID:
           pa_repo = p
           break
    if pa_repo is None:
        raise Exception("'%s' is not found on local machine" % (PA_YUM_REPO_ID,))
    return os.path.dirname(pa_repo.baseurl[0].strip("/"))


def checkUncompatibleJDK():
    uLogging.info("Checking for incompatible JDK rpms (OpenJDK)...")
    s = uUtil.readCmd(['rpm', '-qa', '*openjdk*', 'jdk'])
    rpms = s.strip().split('\n')
    bad_rpms = filter(lambda p: 'openjdk' in p, rpms)
    if bad_rpms:
        raise uPrecheck.PrecheckFailed('incompatible java rpms found: %s' %
                                       bad_rpms, 'replace with JDK rpm shipped with PA')


def check_mount_bind_bug():
    """Check that is possible to perform "mount --bind" command. This bug appear on PCS containers
    with outdated systemd. It was fixed (POA-99460).
    """

    # It reproducible only on containers with RHEL/CentOS 7.2, so we need to check this.
    if determinePlatform().osverfull[:2] != ('7', '2') or not os.path.isfile("/proc/user_beancounters"):
        return True

    # Trying to bindmount inner_dir to outer_dir, raising an error if "mount --bind" failed.
    outer_dir = "/root/bind_test_dir_outer"
    inner_dir = "/root/bind_test_dir_inner"

    uUtil.execCommand(["mkdir", outer_dir, inner_dir])
    try:
        uLogging.debug("Testing execution of \"mount --bind\" command started. ")
        uUtil.execCommand(["mount", "--bind", outer_dir, inner_dir])
        # Waiting 2 sec after mounting (as pointed out by Vasily Averin),
        # if inner_dir is absent in mtab, umount will return an error.
        time.sleep(2)
        uUtil.execCommand(["umount", inner_dir])
        uUtil.execCommand(["rmdir", outer_dir, inner_dir])
        uLogging.debug("Testing execution of \"mount --bind\" command successfully passed. ")
        return True
    except:
        uUtil.execCommand(["rmdir", outer_dir, inner_dir])
        uLogging.err('Precheck error: "mount --bind" command executed idly.\n')
        import sys
        sys.tracebacklimit = 0
        raise Exception('\nUnable to complete the installation. '
                        'The precheck error occurred: "mount --bind" command executed idly. '
                        'This functionality is critical for the named daemon. If you use Virtuozzo container, '
                        'then probably the systemd version in your OS repository is outdated. In this case, '
                        'you should update your systemd to the version "systemd-219-19.el7_2.3" or higher '
                        'so the "mount --bind" can be executed correctly.')


def installPrerequisites(source_dir):
    return        # all taken from PA yum repo


def initDefaultProps(config):
    config.sysuser = 'pemuser'
    config.sysgroup = 'pemgroup'


def getDefaultRootpath():
    return '/usr/local/pem'


def getRootpath():
    return getDefaultRootpath()        # there is no way to find out non-standard install path on Linux


def yumPackageInstalled(package):
    try:
        yb = yum.YumBase()
        yb.preconf.init_plugins = False
        yb.preconf.debuglevel = 0
        yb.preconf.errorlevel = 0
        if yb.rpmdb.searchNevra(name=package):
            return True
        else:
            return False
    finally:
        yb.closeRpmDB()


def isPOACoreBinariesInstalled():
    return yumPackageInstalled("pa-core") or yumPackageInstalled("pa-agent")


def installPOACoreBinaries(config, progress, build_rpms):
    uLogging.debug("Installing core RPM dependencies")

    uUtil.execCommand(
        ["yum", "-y", "-e", "0", "install", "libselinux-utils", "libgcc.x86_64", "glibc.x86_64", "libgcc.i686", "glibc.i686"])
    uLogging.debug("Installing core RPM files")
    yum_local_install_package(build_rpms)


def yum_local_install_package(rpms, **kwargs):
    __yum_local_deploy_package(rpms, "install", **kwargs)


def yum_local_update_package(rpms, **kwargs):
    __yum_local_deploy_package(rpms, "update", **kwargs)


def __yum_local_deploy_package(rpms, command, **kwargs):
    file_names = []
    for rkey in rpms.keys():
        if rkey == tuple([rpms[rkey]["info"].name, "RHEL", determinePlatform().osverfull[0]]):
            rpm_file = rpms[rkey]['path']
            if not rpm_file:
                if 'strict' in kwargs and kwargs['strict'] is False:
                    continue
                else:
                    raise Exception('Unable to find rpm %s in distribution at %s' % (rpms[rkey]["info"].name, rpm_file))
            file_names.append(rpm_file)

    if file_names:
        command = ["yum", "-y", "--nogpgcheck", command] + file_names
        try:
            uUtil.execCommand(command)
        except uUtil.ExecFailed as e:
            uLogging.err("""Failed to install [%s]:\n
            Command '%s' failed\n
            Check if YUM is properly installed and configured, note that you can use option --repo-base-url to
            customize Central PA YUM URL.""" % (' '.join(file_names),  " ".join(command)))
            raise e


def removePOACoreBinaries():
    uUtil.execCommand(["yum", "-y", "-e", "0", "remove", "pleskd", "pa-agent", "poa-core", "pa-core"])
    # in PPA, billing is on MN
    uUtil.execCommand(["yum", "-y", "-e", "0", "remove", "bm"])

    service_control('stop', 'httpd', valid_codes=[0, 1])
    uLogging.debug('Trying to remove old *.conf from /etc/httpd/conf.d/ (if exists)')  # POA-91898
    shutil.rmtree('/etc/httpd/conf.d', ignore_errors=True)
    uUtil.execCommand(["yum", "-y", "-e", "0", "reinstall", "httpd", "mod_ssl", "php"])
    service_control('start', 'httpd')
    removeBilling()


def removeBilling():
    uLogging.debug('stopping billing, if any')
    service_control('stop', 'pba', valid_codes=[0, 1, 255])
    billing_services = ['scheduler', 'generic_worker', 'atm', 'logsrv', 'ssm', 'www', 'xmlrpcd']
    for service in billing_services:
        try:
            uUtil.execCommand(['killall', service])
            uUtil.execCommand(['killall', '-9', service])
        except:
            pass
    uLogging.debug('removing billing rpms, if any')
    uUtil.execCommand('yum -y -e 0 remove bm patools bm-*')

    bm_path='/usr/local/bm'
    if os.path.isdir(bm_path):
        uLogging.debug('Removing billing folder %s' % bm_path)
        shutil.rmtree(bm_path)


def translatePlatformForRPMManagement(platform):
    opsys, osver, oparch = platform.os, platform.osver, platform.arch
    if opsys == 'CentOS':
        opsys = 'RHEL'
    return opsys, osver, oparch


def inVZContainer():
    return os.path.exists('/proc/user_beancounters') and not os.path.exists('/etc/virtuozzo-release')

redhat_release_pattern = re.compile(r"(.+) release (\d+).*")


mempattern = re.compile("MemTotal:\s+(\d+) kB")


def getMemTotal():
    f = open("/proc/meminfo")
    try:
        all_mem = sh_mem = None
        for line in f:
            lm = mempattern.match(line)
            if lm:
                all_mem = int(lm.group(1)) * 1024

        if inVZContainer():
            f.close()
            privvmpages = shmpages = None
            f = open("/proc/user_beancounters")
            for line in f.readlines():
                cols = line.split()
                if cols and len(cols) > 4:
                    if cols[0] == 'privvmpages':
                        privvmpages = int(cols[4])
                    elif cols[0] == 'shmpages':
                        shmpages = int(cols[4])

            if shmpages:
                sh_mem = min(all_mem, shmpages * 4096)
            if privvmpages:
                all_mem = min(all_mem, privvmpages * 4096)

        if not sh_mem:
            sh_mem = all_mem
        if all_mem:
            return all_mem, sh_mem

    finally:
        f.close()

    raise Exception("Cannot find MemTotal line in /proc/meminfo")


def adjustMemSettings(product_name, mem):
    def edit_sysctl_conf(infile, outfile, invz, mem):
        changed_all = changed_max = False
        for ln in infile:
            l = ln.split('=', 1)
            if len(l) == 2:
                p, v = l
                p = p.strip()
                if p == 'kernel.shmall':
                    print >> outfile, 'kernel.shmall = %s' % mem
                    changed_all = True
                elif p == 'kernel.shmmax':
                    print >> outfile, 'kernel.shmmax = %s' % mem
                    changed_max = True
                elif invz and p == "net.ipv4.tcp_syncookies":
                    print >> outfile, '#', ln.strip()
                else:
                    outfile.write(ln)
            else:
                outfile.write(ln)
        if not changed_all or not changed_max:
            print >> outfile, "# Required by", product_name
        if not changed_all:
            print >> outfile, 'kernel.shmall = %s' % mem
        if not changed_max:
            print >> outfile, 'kernel.shmmax = %s' % mem
    uUtil.editFileSafe('/etc/sysctl.conf', edit_sysctl_conf, '/etc/sysctl.conf.pasave', inVZContainer(), mem)
    uUtil.execCommand(["sysctl", "-p"], [0, 255])

addr_pattern = re.compile(r"^\s*inet ([.\d]+)/(\d+) .* scope .* (\S+)$")

def listNetifaces():
    from netaddr import IPNetwork
    out = uUtil.readCmd(['ip', 'addr'])
    rv = []
    for line in out.splitlines():
        m = addr_pattern.match(line)
        if m:
            ip = m.group(1)
            mask = m.group(2)       # digit
            if_name = m.group(3)
            n = IPNetwork(ip + '/' + mask)
            rv.append((if_name, ip, str(n.netmask)))
    return rv

httpd_conf_dir = "/etc/httpd/conf.d"


def createPPMSite(config):
    p = determinePlatform()
    if p.osver == '7':
        allow_directive = '    Require all granted'
    else:
        allow_directive = """
    Order allow,deny
    Allow from all
"""

    conf = open(os.path.join(httpd_conf_dir, "pa_tarballs.conf"), "w")
    tdir = os.path.join(config.rootpath, "install", "tarballs")
    if not tdir.endswith('/'):
        tdir += '/'
    conf.write("""Alias /tarballs/ "%(dir)s"

<Directory "%(dir)s">
    Options Indexes MultiViews
    AllowOverride None
%(allow_dir)s
</Directory>
""" % { "dir" : tdir, "allow_dir" : allow_directive} )
    conf.close()
    try:
        os.makedirs(tdir)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise

    os.path.walk(config.source_dir, uPackaging.copy_tarballs, tdir)
    service_control('reload', 'httpd')


def create_ssl_cert(config):
    uLogging.debug('Creating self-signed SSL certificate')
    ssl_path = os.path.join(config.rootpath, 'etc', 'ssl')
    if not os.path.exists(ssl_path):
        os.makedirs(ssl_path, 0755)

    key_file = os.path.join(ssl_path, 'ppa_ssl_proxy_key.pem')
    cert_file = os.path.join(ssl_path, 'ppa_ssl_proxy_cert.pem')
    cmd = '%s req -x509 -newkey rsa:2048 -keyout %s -out %s -days 366 -nodes -subj "/CN=%s"' % (
        uCrypt._get_openssl_binary(), key_file, cert_file, config.external_ip)
    uUtil.execCommand(cmd)
    os.chmod(key_file, 0600)
    os.chmod(cert_file, 0600)
    return key_file, cert_file


def create_ssl_proxy(config, port=9443):
    uLogging.info('Creating SSL proxy')
    uUtil.execCommand(["yum", "-y", "install", "mod_ssl"])
    key, cert = create_ssl_cert(config)
    vh = ssl_proxy_vhost_template % {
        'host': socket.gethostname(), 'port': port, 'ip': config.external_ip, 'key': key, 'cert': cert}
    conf_file = 'ppa_ssl_proxy.conf'
    f = open(os.path.join(httpd_conf_dir, conf_file), "w")
    f.write(vh)
    f.close()
    service_control('restart', 'httpd')


def createTMLOGSSite(rootpath, ip):
    conf = open(os.path.join(httpd_conf_dir, "tmlogs.conf"), "w")
    tdir = os.path.join(rootpath, 'var', 'taskLogs')
    if not tdir.endswith('/'):
        tdir += '/'
    conf.write("""Alias /tmlogs/ "%(dir)s"

<Directory "%(dir)s">
    Options MultiViews
    AllowOverride None
    <IfVersion < 2.4.5 >
        Order allow,deny
        Allow from all
    </IfVersion>
    <IfVersion >= 2.4.5 >
        Require all granted
    </IfVersion>
</Directory>
""" % { "dir" : tdir} )
    conf.close()
    # Folder created in /u/ bootstrap.py
    service_control('reload', 'httpd')

def configureSystem(config, progress, PRODUCT):
    from poaupdater.uAction import retriable
    progress.set_progress(0, "Disable SELinux")
    selinux_mode = uUtil.readCmd(['getenforce']).strip()
    if 'Enforcing' == selinux_mode:
        uLogging.debug("SELinux is %s. Disabling..." % selinux_mode)
        uUtil.readCmd(['setenforce', '0'])
        selinux_line_pattern = re.compile(r"SELINUX\s*=.*")
        selinux_config = "/etc/selinux/config"
        if os.path.exists(selinux_config):
            def disable_selinux(inf, outf):
                for ln in inf.readlines():
                    if selinux_line_pattern.match(ln):
                        outf.write('SELINUX=disabled\n')
                    else:
                        outf.write(ln)
            uUtil.editFileSafe(selinux_config, disable_selinux, selinux_config + ".pemsave")
    else:
        uLogging.debug("SELinux is %s" % selinux_mode)

    progress.set_progress(3, "Adjusting kernel memory settings")
    all_mem, sh_mem = getMemTotal()
    adjustMemSettings(PRODUCT, all_mem)

    gname = config.sysgroup
    uname = config.sysuser
    progress.set_progress(2, "Creating group %s" % gname)
    try:
        grpinfo = grp.getgrnam(gname)
    except KeyError:
        retriable(uUtil.execCommand)(["groupadd", "-f", gname])
        grpinfo = grp.getgrnam(gname)
    config.sysgroupid = grpinfo.gr_gid

    progress.set_progress(5, "Creating user %s" % uname)
    try:
        pwdinfo = pwd.getpwnam(uname)
    except KeyError:
        retriable(uUtil.execCommand)(["useradd", "-M", uname, "-g", gname, "-d", "/nonexistent", "-s", "/bin/false"])
        pwdinfo = pwd.getpwnam(uname)
    config.sysuserid = pwdinfo.pw_uid
    config.sysgroupid = pwdinfo.pw_gid

    uLogging.debug("dump config after configureSystem: %s" % uUtil.stipPasswords(vars(config)))
    uLogging.debug("Setting hostname")

    hostname_line_pattern = re.compile(r"HOSTNAME=.*")

    def sysconfig_network(inf, outf):
        for ln in inf.readlines():
            if hostname_line_pattern.match(ln):
                print >> outf, 'HOSTNAME="%(hostname)s"' % vars(config)
            else:
                outf.write(ln)
    uUtil.editFileSafe('/etc/sysconfig/network', sysconfig_network, '/etc/sysconfig/network.pemsave')
    uUtil.execCommand(["hostname", config.hostname])

    #install httpd + modules
    httpd_rpms = ["httpd", "mod_ssl"]

    platform = determinePlatform()
    if platform.osver == "6":
        httpd_rpms.append("mod_proxy_wstunnel")

    uUtil.execCommand(["yum", "-y", "-e", "0", "install"] + httpd_rpms)

    def tweak_httpd(inf, outf):
        conf_orig = inf.read()
        conf_1 = re.sub("ServerLimit.*10", "ServerLimit 256", conf_orig)
        conf_2 = re.sub("MaxClients.*10", "MaxClients 256", conf_1)
        outf.write(conf_2)
    prefork_conf = '/etc/httpd/conf/httpd.conf'
    if os.path.exists('/etc/httpd/conf.d/mpm_prefork.conf'):
        prefork_conf = '/etc/httpd/conf.d/mpm_prefork.conf'
    uUtil.editFileSafe(prefork_conf, tweak_httpd, prefork_conf + '.save')

    # drop the /icons/ alias; =<2.2.*: tweak httpd.conf, >=2.4.*: rename autoindex.conf
    if os.path.exists('/etc/httpd/conf.d/autoindex.conf'):
        uUtil.moveFile('/etc/httpd/conf.d/autoindex.conf', '/etc/httpd/conf.d/autoindex.conf.save')
    else:
        uUtil.replaceInFile('/etc/httpd/conf/httpd.conf', r'(?m)^\s*Alias\s+\/icons\/\s+', '#Alias /icons/ ', True)

    service_control('restart', 'httpd')
    service_autostart('httpd')

    if platform.osver == "7":
        def tweak_journald(inf, outf):
            conf_orig = inf.read()
            conf_1 = re.sub('#RateLimitInterval.*s', 'RateLimitInterval=0', conf_orig)
            conf_2 = re.sub('#Storage=auto', 'Storage=none', conf_1)
            conf_3 = re.sub('#ForwardToSyslog=no', 'ForwardToSyslog=yes', conf_2)
            outf.write(conf_3)
        journald_conf = '/etc/systemd/journald.conf'
        uUtil.editFileSafe(journald_conf, tweak_journald, journald_conf + '.save')

        def tweak_rsyslog(inf, outf):
            conf_orig = inf.read()
            conf_1 = re.sub('\$ModLoad imjournal', '#$ModLoad imjournal', conf_orig)
            conf_2 = re.sub('\$IMJournalStateFile imjournal.state', '#$IMJournalStateFile imjournal.state', conf_1)
            conf_3 = re.sub('\$OmitLocalLogging on', '$OmitLocalLogging off', conf_2)
            outf.write(conf_3)
        rsyslog_conf = '/etc/rsyslog.conf'
        uUtil.editFileSafe(rsyslog_conf, tweak_rsyslog, rsyslog_conf + '.save')

        def tweak_unix_datagram_query_len(inf, outf):
            """Tweak unix datagram query length for properly forwarding logs from journald to rsyslog"""
            conf_orig = inf.read()
            qlen_regex = re.compile('\nnet\.unix\.max_dgram_qlen.*')
            if qlen_regex.search(conf_orig):
                conf_modified = qlen_regex.sub('\nnet.unix.max_dgram_qlen=500', conf_orig)
            else:
                conf_modified = conf_orig + "\nnet.unix.max_dgram_qlen=500\n"
            outf.write(conf_modified)

        sysctl_conf = "/etc/sysctl.conf"
        uUtil.editFileSafe(sysctl_conf, tweak_unix_datagram_query_len, sysctl_conf + ".save")

        # Restart all tweaked services
        service_control('restart', 'systemd-journald')
        service_control('restart', 'rsyslog')
        uUtil.execCommand(["sysctl", "-p"], [0, 255])

def create_pgslq_certificates(run, data_dir):
    openssl_cmd = "'openssl req -nodes -new -x509 -days 36500 -keyout %s/server.key -out %s/server.crt -subj \"/C=RU/O=Odin/CN=OA System Database Master Endpoint\"'" % (data_dir, data_dir)
    run("su - postgres -c " + openssl_cmd)
    run("chmod 0600 %s/server.key" % data_dir)

def tunePostgresLogs(run):
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_destination', r"\'stderr\'"))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('logging_collector', 'on'))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_truncate_on_rotation', 'on'))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_rotation_age', r"\'1d\'"))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_filename', r"\'postgresql-%a.log\'"))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_checkpoints', 'on'))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_connections', 'on'))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_disconnections', 'on'))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_error_verbosity', 'terse'))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_line_prefix', r"\'[%m] p=%p:%l@%v c=%u@%h/%d:%a \'"))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_lock_waits', 'on'))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('deadlock_timeout', r"\'1s\'"))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_min_duration_statement', r"\'5s\'"))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_autovacuum_min_duration', r"\'60s\'"))
    run("""su - postgres -c "psql -t -P format=unaligned -c $'alter system set %s = %s '" """ % ('log_temp_files', r"\'1MB\'"))

def configureDatabaseImpl(pgsql, config, access_ips=[]):
    '''access_ips must be of  type [(ip, auth_method)] and provide additional ip-based access to database.'''
    if pgsql is None:
        pgsql = uPgSQL.PostgreSQLConfig(version = uPgSQL.TargetPostgreSQLVersion, commander = config.__dict__.get("commander"))
        pgsql.cleanup()
        pgsql.init_db()

    run = pgsql.get_commander()
    uLogging.info("Configuring installed PostgreSQL %s ..." % pgsql.get_version())
    pg_hba = pgsql.get_pghba_conf()
    pg_conf = pgsql.get_postgresql_conf()
    pg_conf_new  = pg_conf + ".new"

    uLogging.info("Calculating available memory...")
    all_mem = long(run("sed -n -E 's|MemTotal:[ \\t]+([1-9][0-9]{0,9}) kB|\\1|p' /proc/meminfo").strip())*1024
    #check we deal with VZ container
    if run("if [ -f /proc/user_beancounters ] && [ ! -f /etc/virtuozzo-release ]; then echo vz; fi;").strip() == "vz":
        cols = run("cat /proc/user_beancounters | grep -E '[ \\t]*privvmpages([ \\t]+[0-9]{0,19}){5}$'").split()
        if len(cols) == 5:
            all_mem = min(all_mem, long(cols[4])*4096)

    uLogging.info("Running pgtune to generate target postgresql configuration file with respect to available memory...")
    uLogging.info("Available memory = "+str(all_mem/1024)+ "kB")
    run("pgtune -i "+pg_conf+" -c 256 -M"+str(all_mem/2)+" -S /usr/share/pgtune -o "+pg_conf_new)

    if pgsql.get_version_as_int() >= 95:
        #Replacing wrong configuration parameter in advance - server does not recognize checkpoint_segments, setting max_wal_size is used instead
        checkpoint_segments = int(run("sed -n -E 's|[ \\t]*checkpoint_segments[ \\t]*=[ \\t]*([1-9][0-9]{0,8}).*|\\1|p' "+pg_conf_new).strip())
        max_wal_size = "max_wal_size = "+str((3*checkpoint_segments)*16)+" # PA Installer "
        run("sed -i -E 's|[ \\t]*checkpoint_segments[ \\t]*=[ \\t]*([1-9][0-9]{0,8}).*|"+max_wal_size+"|g' "+pg_conf_new)

    #Setting listening address wildcard for the newly installed server, config is just appended
    run("sed -i -E 's|[ \\t\\#]*(listen_addresses[ \\t]*=[ \\t]*)|\\1\\x27*\\x27 # PA Installer |g' "+pg_conf_new)
    #Setting bytea_output format, config is just appended
    run("sed -i -E 's|[ \\t\\#]*(bytea_output[ \\t]*=[ \\t]*)|\\1\\x27escape\\x27 # PA Installer |g' "+pg_conf_new)

    run("sed -i -E 's|[ \\t\\#]*(wal_level[ \\t]*=[ \\t]*)|\\1hot_standby # PA Installer |g' "+pg_conf_new)
    run("sed -i -E 's|[ \\t\\#]*(max_wal_senders[ \\t]*=[ \\t]*)|\\116 # PA Installer |g' "+pg_conf_new)
    run("sed -i -E 's|[ \\t\\#]*(ssl[ \\t]*=[ \\t]*)|\\1on # PA Installer |g' "+pg_conf_new)
    #Actializing new postgresql configuration by moving to original name
    run("mv -f "+pg_conf_new+" "+pg_conf)

    #Replace ident by md5 for local IPv6
    run("sed -i -E 's|([ \\t]*host[ \\t]+all[ \\t]+all[ \\t]+::1/128[ \\t]+).*|\\1md5|g' "+pg_hba)
    #Replace ident by md5 for local IPv4
    run("sed -i -E 's|([ \\t]*host[ \\t]+all[ \\t]+all[ \\t]+127\.0\.0\.1/32[ \\t]+).*|\\1md5|g' "+pg_hba)
    #Remove peer for local sockets for all users but postgres
    run("sed -i -E 's|([ \\t]*local[ \\t]+all[ \\t]+)(all)([ \\t]+.*)|\\1postgres\\3|g' "+pg_hba)

    #Odin Automation prolog
    run("sed -i -e '$,+0a\\# Odin Automation required parameters (BEGIN)' "+pg_hba)

    #Trusting communication IPs
    params = [{'ip_address': ip, 'method':auth_method} for ip, auth_method in access_ips if ip and auth_method]
    for p in params:
        trusted_cfg_text = (uPgSQL.pg_hba_conf_tail_template % p)
        for s in trusted_cfg_text.splitlines():
            run("sed -i -e '$,+0a\\%s' %s" % (s, pg_hba))

    #Create ssl certificates
    create_pgslq_certificates(run, pgsql.get_data_dir())

    #Odin Automation epilog
    run("sed -i -e '$,+0a\\# Odin Automation required parameters (END)' "+pg_hba)
    uLogging.info("Configuring has finished!")

    uLogging.info("Starting PostgreSQL service ...")
    #Starting newly installed server
    pgsql.start()
    uLogging.info("PostgreSQL service has started!")

    uLogging.info("Post-configuring installed PostgreSQL to be started on OS start up ")
    #server has been started successfully so that
    #we mark server to be started on OS start up
    pgsql.set_autostart()
    uLogging.info("Post-configuration has finished!")

    uLogging.info("Starting Postgresql logging configuration")
    tunePostgresLogs(run)
    pgsql.reload()
    uLogging.info("Postgresql logging configuration finished")


def configureDatabase(config, progress, access_ips=[]):
    configureDatabaseImpl(None, config, access_ips)


def upgradeDatabase(config, progress, access_ips):
    #with the potential customized commander that could operate on remote PostgreSQL server

    #postgresql version to upgrade onto
    trgVer = config.__dict__.get("target_postgresql_version")
    if trgVer is None:
        trgVer = uPgSQL.TargetPostgreSQLVersion

    #current one
    pgsqlOrgn = uPgSQL.PostgreSQLConfig(commander = config.__dict__.get("commander"))
    #next version
    run = pgsqlOrgn.get_commander()
    pgsqlNext = uPgSQL.PostgreSQLConfig(version = trgVer, commander = run)

    #check version is the same or even newer
    if pgsqlOrgn.get_version_as_int() >= pgsqlNext.get_version_as_int():
        uLogging.info("PostgreSQL runs required or newer version %s, nothing to uprade." % (pgsqlOrgn.get_version(),))
        return False#so nothing to upgrade

    uLogging.info("Stopping original server...")
    pgsqlOrgn.stop(True)
    uLogging.info("Original server has stopped!")

    uLogging.info("Initilizing database...")
    pgsqlNext.cleanup()
    pgsqlNext.init_db()
    uLogging.info("Database has been initialized!")

    uLogging.info("Upgrading databases...")
    oldBinDir = pgsqlOrgn.get_bin_dir()
    oldDataDir = pgsqlOrgn.get_data_dir()
    newBinDir = pgsqlNext.get_bin_dir()
    newDataDir = pgsqlNext.get_data_dir()

    orgnlConf = None
    orgnlConfBu = None
    if pgsqlOrgn.get_version() == "9.0":
        if pgsqlNext.get_version_as_int() >= 95:
            orgnlConf = pgsqlOrgn.get_postgresql_conf()
            orgnlConfBu = orgnlConf + "-orig"
            run("cp -f "+orgnlConf+" "+orgnlConfBu)
            #this is required unless the unix domain socket would not be found by new pg_upgrade
            run("sed -i -E 's|[ \\t\\#]*(unix_socket_directory[ \\t]*=[ \\t]*)|\\1\\x27/var/run/postgresql/\\x27 # PA Installer |g' "+orgnlConf)
    elif pgsqlOrgn.get_version() == "9.2":
        #check unix_socket_directories is presented in the config file - this is used as a marker of patched RHEL postgresql-9.2
        #for more details see https://jira.int.zone/browse/POA-105930
        socket_dirs_cfg_is_presented = run("grep -E '\\bunix_socket_directories\\b' %s 2> /dev/null || echo -n" % pgsqlOrgn.get_postgresql_conf()).strip()
        if socket_dirs_cfg_is_presented:
            orgnlConf = oldBinDir+"/pg_ctl"
            orgnlConfBu = orgnlConf+"-orig"
            run("mv -f "+orgnlConf+" "+orgnlConfBu)
            run("echo '#!/bin/bash' > " + orgnlConf)
            run("echo '\"$0\"-orig \"${@/unix_socket_directory/unix_socket_directories}\"' >> " + orgnlConf)
            run("chmod +x " + orgnlConf)
    try:
        cmdUpgrade = "'%s/pg_upgrade --new-port=8352 --old-port=8352 --old-bindir=%s --new-bindir=%s --old-datadir=%s --new-datadir=%s'" % (newBinDir, oldBinDir, newBinDir, oldDataDir, newDataDir)
        run("su - postgres -c "+ cmdUpgrade)
    finally:
        if orgnlConfBu:
            #restoring original configuration file
            run("mv -f "+orgnlConfBu+" "+orgnlConf)
    uLogging.info("Upgrading databases has fnished!")

    #performing post upgrade configuration
    configureDatabaseImpl(pgsqlNext, config, access_ips)
    return True


def cleanUpOldPostgresRPMs(run):
    currConfig = uPgSQL.PostgreSQLConfig(commander = run)
    run = currConfig.get_commander()
    vergrp = "|".join(["".join(i.split(".")) for i in uPgSQL.SupportedPostgreSQLVersions])
    postgreSqlRPMs = run("rpm -qa --queryformat '%{name}.%{arch}\\n' | grep -E '^postgresql("+vergrp+")??(\\-(libs|upgrade|server))??\\.(x86_64|i686)'").strip().splitlines()
    if not postgreSqlRPMs:
        return
    postgresqlDirsToErase = []
    postgreSqlRPMsToErase = []
    currVersion = currConfig.get_version_as_int()
    for rpmName in postgreSqlRPMs:
        rpmVer = run("rpm -q --queryformat '%{version}' "+rpmName).strip()
        rpmVer = ".".join(rpmVer.split(".")[:2])
        rpmVerInt = int("".join(rpmVer.split(".")))
        if rpmVerInt < currVersion:
            if "-server" in rpmName:
                postgresqlDirsToErase.append("/usr/pgsql-"+rpmVer)
            postgreSqlRPMsToErase.append(rpmName)
    if postgreSqlRPMsToErase:
        #On (CentOS|RHEL)6 PA is 32 bits application thus it continues depending on postgresql95-libs.i686
        if currConfig.get_os_version().startswith("6."):
            if 'postgresql95-libs.i686' in postgreSqlRPMsToErase:
                postgreSqlRPMsToErase.remove('postgresql95-libs.i686') #That's why we excluding this RPM from the list to erase
        if postgreSqlRPMsToErase:
            run("rpm -e --nodeps "+" ".join(postgreSqlRPMsToErase))
    for d in postgresqlDirsToErase:
        run("rm -fr "+d)
    #checking psql tool is available
    #for more details see following issue https://jira.int.zone/browse/POA-105993
    psqlOk = True
    try:
        run("psql --version 2> /dev/null")
    except:
        psqlOk = False
    if not psqlOk:
        #postCleanUpRepairRPMs callable object is assigned by 173000-APS-34471-PostgreSQL-91-to-96-migration.py upgrade action
        #it happens when we deal with the PostgreSQL server running remotely - in such a case DB node cna not connect to pa-repo yum repository
        #that's why it is required to run very special remote commad to repair postgresql RPM
        #for more details see this bug https://jira.int.zone/browse/POA-109067
        repairRPMs = run.__dict__.get("postCleanUpRepairRPMs") 
        if repairRPMs is None:
            run("yum -y reinstall postgresql"+str(currVersion).replace(".", ""))
        else:
            repairRPMs()


def configureODBC(config):
    pgsql = uPgSQL.PostgreSQLConfig()
    path = os.path.join(config.rootpath, 'etc')
    params = {'driver': pgsql.get_odbc_driver(), 'setup': pgsql.get_odbc_driver(),
              'dsn': config.dsn, 'database_name': config.database_name,
              'database_host': config.database_host, 'database_port': config.database_port}
    for fn, template in [('odbc.ini', uPgSQL.odbc_ini_template), ('odbcinst.ini', uPgSQL.odbcinst_ini_template)]:
        f = open(os.path.join(path, fn), 'w')
        f.write(template % params)
        f.close()


def __set_uid_by_user(user):
    from pwd import getpwnam
    os.seteuid(getpwnam(user).pw_uid)


def __db_connect_local(user, database_name):
    uLogging.debug('connecting to database locally, user %s, database_name %s' % (user, database_name))
    import psycopg2
    # connecting use peer authentication
    __set_uid_by_user(user)
    max_tries = 60
    i = 0
    while True:
        try:
            admin_con = psycopg2.connect(user=user, database=database_name)
            break
        except psycopg2.OperationalError as e:
            i += 1
            uLogging.debug('error %s, try %s of %s' % (e, i, max_tries))
            if i < max_tries:
                time.sleep(1)
                continue
            uLogging.debug('max retries exceeded, rethrow')
            raise e
    admin_con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return admin_con

# temporary, until billing propagation
def createDB(config, askYesNo, clean=False):
    create_db(config, askYesNo, clean)


def create_db(config, askYesNo, clean=False):
    # set flag 'clean' to True if you need to create 'clean' db based on template0,
    # else db based on template1 will be created with predefined table 'dual' and language 'plpgsql', that is ususal way
    admin_con = __db_connect_local(admin_db_user, admin_db)
    try:
        cur = admin_con.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datname='%s'" % config.database_name)
        if cur.fetchone():
            if config.reinstall or askYesNo("Database '%s' already exist, drop it?" % config.database_name):
                cur.execute("DROP DATABASE %s" % config.database_name)
            else:
                raise Exception("Database %s already exists" % config.database_name)
        cur.execute(
            "CREATE OR REPLACE FUNCTION plpgsql_call_handler() RETURNS language_handler AS 'plpgsql.so' LANGUAGE 'c'")
        cur.execute("SELECT 1 FROM pg_language WHERE lanname='plpgsql'")
        if not cur.fetchone():
            cur.execute(
                "CREATE TRUSTED PROCEDURAL LANGUAGE 'plpgsql' HANDLER plpgsql_call_handler LANCOMPILER 'PL/pgSQL';")

        cur.execute("SELECT 1 FROM pg_class WHERE relname='dual'")
        if not cur.fetchone():
            cur.execute("CREATE TABLE dual(dummy CHAR(1))")
        cur.execute("TRUNCATE dual")
        cur.execute("INSERT INTO dual VALUES('X')")
        cur.execute("GRANT SELECT ON dual TO PUBLIC")
        cur = admin_con.cursor()
        template = 'template1'
        if clean:
            template = 'template0'
        cur.execute("CREATE DATABASE %s ENCODING 'UTF8' TEMPLATE %s" % (config.database_name, template))
        cur.execute("SELECT usesysid FROM pg_shadow WHERE usename = '%s'" % config.dsn_login)
        row = cur.fetchone()
        if not row:
            cur.execute("CREATE USER %s WITH PASSWORD '%s' NOCREATEDB; ALTER ROLE %s SET transform_null_equals TO on;" % (config.dsn_login, config.dsn_passwd, config.dsn_login))
            cur.execute("SELECT usesysid FROM pg_shadow WHERE usename = '%s'" % config.dsn_login)
            row = cur.fetchone()
        dbuser_oid = row[0]
        cur.execute("UPDATE pg_database SET datdba = '%s' WHERE datname = '%s'" % (dbuser_oid, config.database_name))
        if config.dsn_passwd:
            cur.execute("ALTER USER %s WITH ENCRYPTED PASSWORD '%s';" % (config.dsn_login, config.dsn_passwd))
    finally:
        __set_uid_by_user('root')
        admin_con.close()


def dropDB(config):
    __set_uid_by_user(admin_db_user)
    try:
        import psycopg2 as _dbmodule
        admin_con = _dbmodule.connect(user=admin_db_user, database=admin_db)
        admin_con.set_isolation_level(_dbmodule.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    except ImportError: #TODO: anikulin, for precheck, remove in 7.1
        from pyPgSQL import PgSQL as _dbmodule
        admin_con = _dbmodule.connect(host=config.database_host, user=admin_db_user, database=admin_db)
        admin_con.autocommit = True

    cur = admin_con.cursor()
    cur.execute("DROP DATABASE %s" % config.database_name)
    __set_uid_by_user('root')


def dumpDB(config, path):
    command_to_log = 'pg_dump --username=%s --host=%s --port=%s -Fc %s > %s' % (config.dsn_login, config.database_host, config.database_port, config.database_name, path)
    command = 'PGPASSWORD="%s" %s ' % (config.dsn_passwd, command_to_log)
    uUtil.execCommand(command=command, command_to_log=command_to_log)

def restoreDB(config, path, askYesNo=uDialog.askYesNo):
    create_db(config, askYesNo, True)
    command_to_log = 'pg_restore --username=%s --host=%s --port=%s -Fc -n public -d %s %s' % (config.dsn_login, config.database_host, config.database_port, config.database_name, path)
    command = 'PGPASSWORD="%s" %s ' % (config.dsn_passwd, command_to_log)
    uUtil.execCommand(command=command, command_to_log=command_to_log)

def hasNoexec(checkedDir):
    dirPath = os.path.abspath(checkedDir)
    hasNoexec = False
    records = open('/proc/mounts', 'r')
    for record in records.readlines():
        columns = record.split()
        if len(columns) < 4: continue
        mountDir = columns[1]
        if mountDir != dirPath: continue
        mountOptions = columns[3]
        hasNoexec = 'noexec' in mountOptions
        if hasNoexec: break
    uLogging.debug('Directory %s has "noexec" mount option: %s' % (dirPath, hasNoexec))
    return hasNoexec

def checkNoexec(checkedDir):
    uLogging.info('Checking "noexec" option for directory: %s' % checkedDir)
    if hasNoexec(checkedDir):
       raise uPrecheck.PrecheckFailed('directory "%s" has "noexec" mount option' % checkedDir, 'remount "%s" directory without "noexec" option' % checkedDir)

def checkTmpDirectory():
    # Directory "/tmp"  with "noexec" mount option is cause fail of pau during startup becouse of pacryptoapi (JNI).
    checkNoexec('/tmp')


def get_java_home():
    return os.getenv('JAVA_HOME', "/usr/java/default")


def get_default_log_file():
    return '/var/log/pa/install.log'


def getDefaultMigrateLogFile():
    return '/var/log/pa/migrate.log'
