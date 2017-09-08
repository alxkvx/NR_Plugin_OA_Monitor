import os
import re
import uUtil
import uLogging
import uPrecheck
from uConst import Const


def checkDiskSpace(path, required, free):
    if free < required:
        raise Exception("%s has less than %s GB free disk space: %s GB" % (path, required, free))
    uLogging.debug("Available free space in %s ~%s GB" % (path, free))


def __get_java_version():
    java_path = 'java'
    if Const.isWindows():
        java_path = os.path.join(os.environ["JAVA_HOME"], 'bin', 'java')
    uLogging.debug('checking java version by path: %s' % java_path)

    todo = 'ensure JDK installed and java executable is in PATH (linux) or in JAVA_HOME/bin/ (windows)'
    try:
        out, err, ret = uUtil.readCmdExt([java_path, '-version'])
    except Exception, e:
        raise uPrecheck.PrecheckFailed('can not check java version! exception %s' % e, todo)
    if ret != 0:
        raise uPrecheck.PrecheckFailed('can not check java version! out %s, err %s, ret %s' % (out, err, ret), todo)
    out = out + err
    uLogging.debug('got java version: %s' % out)
    return out


def _check_java_version(version_str):
    version_str = version_str.lower()
    if ('hotspot' not in version_str) or ('64-bit' not in version_str) or (not re.search('java version \"1\.8\.', version_str)):
        raise uPrecheck.PrecheckFailed('incorrect JDK installed! Oracle HotSpot JDK 1.8 x64 required.',
                                       'install JDK shipped with Operation Automation and ensure java executable is in PATH (linux) or in JAVA_HOME/bin/ (windows)')
    uLogging.debug('java version is correct')


def checkJavaVersion():
    out = __get_java_version()
    _check_java_version(out)


def get_java_binary():
    return os.path.join(getUOSModule().get_java_home(), "bin", "java")


def update_java_tzdata(jdk_tzupdater_path):
    if os.path.isfile(jdk_tzupdater_path):
        # requested http://www.iana.org/time-zones/repository/tzdata-latest.tar.gz
        tzupdater = '%s -jar %s -l' % (get_java_binary(), jdk_tzupdater_path)
        uLogging.debug('Updating java time zones')
        uUtil.execCommand(tzupdater, valid_codes=1)     # tolerate failure if no iana.org access
    else:
        uLogging.debug('%s not found, skipping time zone update' % jdk_tzupdater_path)


def getUOSModule():
    if Const.isWindows():
        import uWindows
        uOS = uWindows
    else:
        import uLinux
        uOS = uLinux
    return uOS
