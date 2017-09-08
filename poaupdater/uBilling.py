import uLogging, uPackaging, uHCL, uUtil, uPrecheck, uPEM, uUtil
import os, sys, re
import uConfig

billingToolsPath = '/usr/local/bm/tools_py/'
mn_config = uConfig.Config()

# w/a for unit tests just to import module
if not hasattr(mn_config, 'communication_ip'):
    mn_config.communication_ip = 'fake'

# Reports from billing hosts are returned as string which can contain several failed precheck with format:
# Precheck error: <trouble description>
# You should: <How to fix trouble>
# precheck.py gets this report from stdout and wrap to uPrecheck.PrecheckFailed to process as usual


class BillingPrecheckFailed(uPrecheck.PrecheckFailed):
    def __init__(self, msg):
        uPrecheck.PrecheckFailed.__init__(self, None, None)
        Exception.__init__(self, msg)
        self.reason = msg


def update_essentials_billing(host_id, bm_packages):

    if filter(lambda x: not is_billing_package(x.package.name), bm_packages):
        raise Exception("Following packages without \"bm\" prefix, but installing as billing packages: {0} ".format(
            str(bm_packages)))

    uLogging.debug("Stopping billing.")
    stop_pba()

    uLogging.debug("Updating billing packages.")
    uPEM.update_packages_on_host(host_id, bm_packages)

    # # Reloading all modules from billingToolsPath TODO: Right way, function stop_pba() is a crutch.
    # modules_for_reload = filter(
    #     lambda x: x is not None and hasattr(x, "__file__") and billingToolsPath in x.__file__,
    #     sys.modules.values())
    # map(reload, modules_for_reload)

    sys.path.append(billingToolsPath)
    import pba
    import configure
    import configure_db
    sys.path.remove(billingToolsPath)


    uLogging_info = uLogging.info
    uLogging_debug = uLogging.debug
    uLogging_log = uLogging.log

    # Disable a lot of messages during billing update
    uLogging.debug = uLogging.log_func(None, uLogging.DEBUG)
    uLogging.info = uLogging.log_func(None, uLogging.INFO)
    uLogging.log = uLogging.log_func(None, None)

    uLogging_info("Starting configure billing. ")
    (configure_db.o, configure_db.args) = configure_db.getOptions(['--unattended', '--ppab'])
    configure_db.main(configure_db.o, configure_db.args)
    uLogging_info("BSS DB configuration has been successfully completed.")
    (configure.o, configure.args) = configure.getOptions(['--unattended'])
    configure.main(configure.o, configure.args)
    uLogging_info("BSS Application configuration has been sucessfully completed.")
    uUtil.execCommand(['sh', '/usr/local/bm/templatestore/tools/configure.sh'])
    uLogging_info("BSS Store configuration has been successfully completed.")
    uLogging_info("Billing configuration has been successfully completed.")

    uLogging_debug("Starting billing.")
    pba.start()()

    # restoring uLogging.* functions
    uLogging.debug = uLogging_debug
    uLogging.info = uLogging_info
    uLogging.log = uLogging_log


class RPM(object):
    name = None
    version = None

    def __init__(self, name, version):
        self.name = name
        self.version = version


class PBABaseConf(object):
    configureScript = None
    startCmd = None
    stopCmd = None
    packages = None
    name = None
    host_id = None
    role = None
    logs = None

    @classmethod
    def get_host_id(cls):
        if not cls.host_id:
            msg=''
            uLogging.debug('Looking for host %s' % cls.name)
            for pkg in cls.packages:
                p = uPackaging.listInstalledPackages(pkg, 'other')
                if p:
                    cls.host_id = p[0].host_id
                    break
                msg += 'No billing host with package %s found!\n' % pkg
            if not cls.host_id:
                raise Exception(msg)
            uLogging.debug('Billing host %s is #%s' % (cls.name, cls.host_id))
        return cls.host_id

    @classmethod
    def configure(cls):
        uLogging.info('running configure')
        request = uHCL.Request(cls.get_host_id(), user='root', group='root')
        request.command(cls.configureScript, stdout='stdout', stderr='stderr')
        output = request.perform()
        uLogging.info('done, output \n%s' % output['stdout'])
        return output

    @classmethod
    def _start_stop(cls, command):
        if not command:
            uLogging.info('...requested operation is not defined for %s' % cls.name)
            return
        request = uHCL.Request(cls.get_host_id(), user='root', group='root')
        request.command(command, stdout='stdout', stderr='stderr')
        output = request.perform()
        uLogging.info('done, output \n%s' % output['stdout'])
        return output

    @classmethod
    def start(cls):
        uLogging.info('Start %s...' % cls.name)
        return cls._start_stop(cls.startCmd)

    @classmethod
    def stop(cls):
        uLogging.info('Stop %s...' % cls.name)
        return cls._start_stop(cls.stopCmd)

    @classmethod
    def getRPMlist(cls):
        bm_pattern='^(bm.*)-(\d+\.\d+\..*)$'
        request = uHCL.Request(cls.get_host_id(), user='root', group='root')
        request.command("rpm -qa", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
        output = request.perform()
        prog=re.compile(bm_pattern)
        res = []
        for x in output['stdout'].split():
            m = prog.match(x)
            if m:
                res.append(RPM(m.group(1), m.group(2)))
        return res

    @classmethod
    def precheck(cls, lpath_to_precheck_tar = None):

        host_id = cls.get_host_id()
        request = uHCL.Request(host_id, user='root', group='root')

        rpath_tmp = '/usr/local/bm/tmp'
        rpath_to_precheck = os.path.join(rpath_tmp, 'precheck')

        if lpath_to_precheck_tar: #else prechecks is assumed already at billing host
            rpath_to_precheck_tar = os.path.join(rpath_tmp, os.path.basename(lpath_to_precheck_tar))

            lpath_to_poaupdater = os.path.dirname(__file__)
            rpath_to_poaupdater = os.path.join(rpath_to_precheck, 'poaupdater')

            uLogging.debug('Transfer %s to %s at host %s' % (lpath_to_precheck_tar, rpath_to_precheck_tar, host_id))
            request.transfer('1', lpath_to_precheck_tar, rpath_tmp)

            uLogging.debug('Remove %s at host %s' % (rpath_to_precheck, host_id))
            request.rm(rpath_to_precheck)

            uLogging.debug('Extract %s to %s at host %s' %(rpath_to_precheck_tar, rpath_to_precheck, host_id))
            request.extract(rpath_to_precheck_tar, rpath_to_precheck)

            uLogging.debug('Transfer %s to %s at host %s' % (lpath_to_poaupdater, rpath_to_precheck, host_id))
            request.mkdir(rpath_to_poaupdater)
            request.transfer('1', lpath_to_poaupdater, rpath_to_poaupdater)

        packages = [x for x in uPackaging.listInstalledPackagesOnHost(host_id) if is_billing_package(x.name)]
        tmp = ' '.join(['-p %s:%s:%s:%s' % (p.name, p.version, '0', 'x86_64') for p in packages])
        rcmd = 'python %s %s %s' % (os.path.join(rpath_to_precheck, 'prechecker.py'), cls.role, tmp)
        uLogging.debug('Launch %s at host %s' %(rcmd, host_id))
        request.command(rcmd, stdout='stdout', stderr='stderr', valid_exit_codes=[0, 1, 2])

        try:
            output = request.perform()

            if output['stdout']:
                raise BillingPrecheckFailed(output['stdout'])

            if output['stderr']:
                uLogging.debug(output['stderr'])
                if 'No such file or directory' in output['stderr']:
                    uLogging.warn('It looks like prechecks were skipped during MN upgrade. Billing prechecks will be skipped too')
                else:
                    raise uPrecheck.PrecheckFailed('Several Billing prechecks were failed at %s (host id #%s).' % (cls.name, host_id), '')

        except uUtil.ExecFailed, e:
            err = str(e)
            uLogging.debug(err)
            if "attribute 'src_host_id' is not declared for element 'TRANSFER'" in err:
                raise uPrecheck.PrecheckFailed('Pleskd agent at %s (host id #%s) has version lower then pleskd on MN. This may be the caused by hosts skip during previous updates.' % (cls.name, host_id), "Update pleskd to 6.0.7 or higher")
            raise uPrecheck.PrecheckFailed('Several Billing prechecks were failed at %s (host id #%s).' % (cls.name, host_id), '')


class PBAConf(PBABaseConf):
    name = 'BSS APP'
    configureScript = '%s/configure.py --unattended' % billingToolsPath
    startCmd = 'python %s/pba.py start' % billingToolsPath  # service pba stop
    stopCmd = 'service pba stop'
    packages = ['PBAApplication', 'PPAB']
    role = 'bm'
    logs = '/var/log/pa/billing_boot.out.log, /var/log/pa/billing_configure.log on BSS APP'

    @classmethod
    def syncLocalesFromDB(cls):
        request = uHCL.Request(cls.get_host_id(), user='root', group='root')
        request.command("python /usr/local/bm/tools_py/configureLocale.py syncLocalesFromDB", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
        output = request.perform()
        uLogging.debug('done, output \n%s' % output['stdout'])

    @classmethod
    def syncStores(cls):
        uLogging.info("Synchronization stores")
        request = uHCL.Request(cls.get_host_id(), user='root', group='root')
        request.command("python /usr/local/bm/tools_py/syncstores.py", stdout='stdout', stderr='stderr', valid_exit_codes=[0])
        output = request.perform()
        uLogging.debug('done, output \n%s' % output['stdout'])
        return output


class PBADBConf(PBABaseConf):
    name = 'BSS DB'
    configureScript = '/usr/local/bm/tools-db/configure_db.py ' \
                      '--unattended --update --mn-backnet-ip=%s' % mn_config.communication_ip
    packages = ['PBADatabase']
    role = 'bm-db'
    logs = '/var/log/pa/billing_configure_db.log on BSS DB'

class PBAStoreConf(PBABaseConf):
    name = 'BSS STORE'
    configureScript = '/usr/local/bm/templatestore/tools/configure.sh'
    packages = ['PBAOnlineStore']
    role = 'bm-templatestore'
    logs = '/var/log/pa/syncstores.log on BSS APP'


def get_billing_hosts():
    res = {}
    for c in PBADBConf, PBAConf, PBAStoreConf:
        try:
            res[c] = c.get_host_id()
        except:
            uLogging.debug('Component %s does not exist' % c.name)
    return res


def is_billing_package(name):
    return name[:3] == 'bm-' or name == 'bm'


def is_scale_down():
    try:
        return PBAConf.get_host_id() == 1
    except Exception, e:
        uLogging.debug(e)
    return False

UpgradeOrder = [PBADBConf, PBAConf, PBAStoreConf]

PackageGroups =  {  'gate'          :   'API Gateways',
                    'cert-plugin'   :   'Certification Plug-ins',
                    'domreg-plugin' :   'Domain Registration Plug-ins',
                    'fraud-checking':   'Fraud Checking Plug-ins',
                    'payment-plugin':   'Payment Plug-ins',
                    'tax-plugin'    :   'Tax calculation Plug-ins',
                    'csc'           :   'Customer-specific Containers',
                    'customization' :   'Additional GUI Customization',
                    'internal'      :   'Internal Packages',
                    'locale'        :   'Language Packs',
                    'sdk'           :   'Plugins SDK',
                    'lib'           :   'Library'}

AttrName = 'BILLING_PACKAGE_GROUP'


def getPackageGroup(pkg_id):
    attrs = uPackaging.getPackageAttributes(pkg_id)
    if AttrName in attrs:
        return attrs[AttrName]
    return None


def check_billing_node_free_disk_space(quota_in_gb):
    """Getting billing nodes id list, checking free disk space requirements"""

    import uPEM
    billing_nodes = get_billing_hosts()
    reasons = []
    for node in billing_nodes:
        try:
            uPEM.check_free_disk_space(node.host_id, quota_in_gb)
        except uPEM.NotEnoughFreeDiskSpace, exception:
            reasons.append(exception.reason)
    if len(reasons) > 0:
        raise uPEM.NotEnoughFreeDiskSpace(errorMessage='\n'.join(reasons))


def precheck(lpath_to_precheck_tar = None):
    billing_hosts_id = get_billing_hosts()
    FailedPrecheck = {}
    for host in [x for x in UpgradeOrder if x in billing_hosts_id]:
        try:
            host.precheck(lpath_to_precheck_tar)
        except uPrecheck.PrecheckFailed, e:
            FailedPrecheck[host] = e
    if FailedPrecheck:
        reasons = ["Composite Billing Precheck failed at %s (host id #%s):\n\n%s" % (host.name, host.host_id, FailedPrecheck[host]) for host in FailedPrecheck]
        raise BillingPrecheckFailed('\n'.join(reasons))


def stop_pba():  # TODO: The right way is to use pba.stop()(). This is crutch for quick resolve PBA-74321.
    uUtil.execCommand(['service', 'pba', 'stop'], [0, 1])
    service_list = ['scheduler', 'generic_worker', 'atm', 'logsrv', 'ssm', 'www', 'xmlrpcd']
    for service in service_list:
        try:
            uUtil.execCommand(['killall', service])
            uUtil.execCommand(['killall', '-9', service])
        except:
            pass
