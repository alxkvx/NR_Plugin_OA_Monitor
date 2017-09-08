import os
import posixpath
import uThreadPool
import uHCL
import uAction
import uLogging
import uPEM
import uUtil
from uConst import Const

class TaskItem:

    def __init__(self, host, pleskd_rpm_url):
        self.host = host
        self.pleskd_rpm_url = pleskd_rpm_url

    def __str__(self):
        return str(self.host)


class UpgradeResultFunctor:

    def __init__(self, task_item, result, fun):
        self.task_item = task_item
        self.host = self.task_item.host
        self.result = result
        self.fun = fun
        self.lastError = None

    def processResult(self):
        if isinstance(self.result, Exception):
            self.lastError = str(self.result)
            raise self.result
        elif 'retcode' in self.result and self.result['retcode'] is not '0' and 'Error: Nothing to do' in self.result['stderr'] and 'does not update installed package' in self.result['stdout']:
            uLogging.warn('pa-agent of desired version is already installed on target host %s' % self.host)
        elif 'retcode' in self.result and self.result['retcode'] is not '0':
            self.lastError = 'return code: %s\n stderr:\n%s\n stdout:\n%s' % (
                self.result['retcode'], self.result['stderr'], self.result['stdout'])
            uLogging.err('Error updating slave %s:\n %s\n' % (self.host, self.lastError))
            self.result = None  # clear result, next retriable should re-run self.fun
            raise Exception("slave node upgrade failed %s" % self.host)
        self.lastError = None

    def __call__(self):
        uLogging.info("process result of slave update: %s" % self.host)
        if not self.result:		# result already obtained in thread pool
            self.result = self.fun(self.task_item)
        self.processResult()
        return True


def preparePool(config):
    fun = lambda task_item: update_host(task_item, config)
    pool = uThreadPool.ThreadPool(fun)
    return pool


def processPoolResults(pool, binfo, config):
    res = pool.get_result()
    while res != None:
        task_item = res[0]
        result = res[1]
        host = task_item.host
        resFun = UpgradeResultFunctor(task_item, result, pool.workerFun)
        pool.condition.acquire()		# pause pool
        try:
            uAction.retriable(resFun)()
        except:		# batch mode or 'abort' selected
            if not config.batch:
                uLogging.info("aborting install, wait for thread pool to terminate")
                pool.terminateLocked()
                raise
        pool.condition.release()

        if resFun.lastError:
            uLogging.info("skipping slave %s upgrade, error: %s" % (host, resFun.lastError))
            binfo.progress.failed_hosts.append((host, resFun.lastError))
        else:
            binfo.progress.updated_hosts.add(host.host_id)
            binfo.saveProgress(config)
            uLogging.info("Slave %s upgrade success" % host)
        res = pool.get_result()


def prepare_request_for_repourl_updating(request, config):
    """Filling the request object with corresponding commands for updating yum repourl

    :param request - uHCL.Request() object
    :param config

    :return request
    """

    yum_repo_url = posixpath.join(config.yum_repo_url, Const.getDistribLinDir(), "$releasever/")
    proxy = "proxy=%s" % config.yum_repo_proxy_url if config.yum_repo_proxy_url else ""
    contents = config.PA_YUM_REPO_CONF_TEMPLATE % {"url": yum_repo_url, "proxy": proxy}
    request.rm("/etc/yum.repos.d/poa.repo")  # remove old poa.repo config file
    request.mkfile(config.PA_YUM_REPO_FILE, contents, owner="root", group="root", perm="0600", overwrite=True)
    request.command("yum clean all --disablerepo=* --enablerepo=pa-central-repo")
    request.command("yum makecache --disablerepo=* --enablerepo=pa-central-repo")
    request.command("yum -q check-update", valid_exit_codes=[0, 100])
    return request


def prepare_request_for_rpm_updating(request, task_item):
    """Filling the request object with corresponding commands for updating yum repourl

    :param request - uHCL.Request() object
    :param task_item
    :param config

    :return request
    """
    rpm_location = task_item.pleskd_rpm_url
    if task_item.host.platform.osver == '5':
        rpm_name = os.path.basename(task_item.pleskd_rpm_url)
        rpm_location = "/tmp/%s" % rpm_name
        request.command("curl %s -o %s" % (task_item.pleskd_rpm_url, rpm_location))
    request.command("rpm --noscripts -e pleskd", valid_exit_codes=[0, 1])
    request.command("yum install --nogpgcheck -y %s" %
                    rpm_location, valid_exit_codes=[0, 1], stdout='stdout', stderr='stderr', retvar='retcode')

    return request


def update_host(task_item, config):
    try:
        # Initializing request object
        request = uHCL.Request(task_item.host.host_id, user='root', group='root')

        # Preparing the instructions if update repourl is necessarily
        if config.need_update_yum_repourl:
            request = prepare_request_for_repourl_updating(request, config)

        # Filling request object with corresponding commands if pleskd_rpm_url is not a fake
        if task_item.pleskd_rpm_url:
            request = prepare_request_for_rpm_updating(request, task_item)

        return request.performCompat()

    except uUtil.ExecFailed, e:
        return {'retcode': e.status, 'stdout': e.out, 'stderr': e.err}


def get_paagent_rpm_path(binfo, platform):
    key = ('pa-agent', platform.os, platform.osver)
    if key in binfo.rpms_to_update:
        r = binfo.rpms_to_update[key]
        return os.path.basename(r['path'])
    return None


def slave_upgrade_paagent_and_repourl(binfo, config):
    """Upgrade pa-agent rpm on all linux slaves

    Parameters:
        :param binfo: uDLModel.BuildInfo
        :param config: uConfig.Config
    """

    # Checking available pa-agent RPM for slave update.
    paagent_in_update = bool(
        [rpm for rpm in binfo.upgrade_instructions.native_packages if 'pa-agent' in rpm.name] or
        filter(lambda r: r[0] == "pa-agent", binfo.rpms_to_update.keys())
    )

    config.need_update_paagent = True

    # Making decision about necessity of pa-agent installation (or necessity to do something).
    if not paagent_in_update and not config.need_update_yum_repourl:
        uLogging.info("No new agent RPMs in build, updating YUM repourl is not needed also, skipping slave update. ")
        return
    elif not paagent_in_update and config.need_update_yum_repourl:
        uLogging.info("No new agent RPMs in build, but updating YUM repourl is necessary. Starting repourl update.")
        config.need_update_paagent = False

    uAction.progress.do("Updating agents on service nodes")

    # Getting slave list, iterate, checking for updatability.
    hosts = uPEM.getHostsWithPleskdRPM()
    hosts_to_update = []

    for host in hosts:
        if host.host_id in binfo.progress.updated_hosts:
            uLogging.info("Skip already updated host %s", host)
        elif host.host_id == 1:
            uLogging.debug("Skip MN host %s" % host)
        else:
            hosts_to_update.append(host)

    if not hosts_to_update:
        uLogging.info("There is no hosts marked for update.")
        uAction.progress.done()
        return

    # Preparing task pool for updating.
    pool = preparePool(config)

    # Filling task pool for slaves
    for host in hosts_to_update:
        uLogging.info("Updating pa-agent on slave %s" % host)
        try:
            # Checking for availability
            if not uPEM.canProceedWithHost(host):
                uLogging.warn("Slave %s is unavailable. " % host)
                continue

            # Setting paagent rpm url. If this will eq None, packages will not be installed
            paagent_rpm_url = None

            # Checking for agent rmp path for platform-specific url
            platform_paagent_rpm_path = get_paagent_rpm_path(binfo, host.platform)

            # Skipping host if pa-agent url is absent and updating YUM repourl is not needed also.
            if not platform_paagent_rpm_path and config.need_update_paagent and not config.need_update_yum_repourl:
                uLogging.debug('No pa-agent RPM for platform %s %s in update distribution, '
                               'updating YUM repourl is not needed also. '
                               'Skipping updating for slave %s' % (host.platform.os, host.platform.osver, host))
                continue

            # Making real pa-agent repourl
            if platform_paagent_rpm_path:
                core_rpms_path = posixpath.join(
                    'corerpms/%s%s' % (Const.getDistribLinDir(), host.platform.osver),
                    os.path.basename(platform_paagent_rpm_path)
                )
                paagent_rpm_url = 'http://' + config.communication_ip + '/tarballs/' + core_rpms_path

            uLogging.debug("Slave '%s' upgrade scheduled. Pa-agent rpm url: '%s'", host, paagent_rpm_url)
            pool.put(TaskItem(host, paagent_rpm_url))

        except uAction.ActionIgnored:
            continue

    threadCount = int(len(hosts_to_update) / 10) + 1

    # Running multithreading update process
    if config.slave_upgrade_threads:
        threadCount = config.slave_upgrade_threads

    pool.start(threadCount)
    uLogging.debug("slave upgrade pool started, %d threads" % threadCount)
    processPoolResults(pool, binfo, config)
    uLogging.info("slave upgrade results collected")
    pool.terminate()
    uLogging.debug("slave upgrade pool terminated")
    uAction.progress.done()


def reportSkippedHosts(binfo):
    if binfo.progress.failed_hosts:
        uLogging.warn("For some slave nodes upgrade was skipped, please upgrade pa-agent manually:")
        for f in binfo.progress.failed_hosts:
            uLogging.warn("%s	%s" % f)


__all__ = ["slave_upgrade_paagent_and_repourl", "reportSkippedHosts"]
