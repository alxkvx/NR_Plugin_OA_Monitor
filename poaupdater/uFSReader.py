import os
import shutil
import string
try:
    import hashlib
except ImportError:
    import md5 as hashlib
import pickle

from uBuild import *
from uManifestParser import *
import uAction
from uRPMUtils import RPMInfo
import MDLReader
import uUtil
import uDialog
from uConst import Const

tarball_pattern = re.compile(r'.+\.(tgz)$')
manifest_pattern = re.compile(r'.+\.pdl\.asc$')

rpm_pattern = re.compile(r'.+\.rpm$')
exe_pattern = re.compile(r'.+\.(exe|msi|dll)$')
mdl_pattern = re.compile(r'.+\.mdl$')
platform_dir_pattern = re.compile(r'^.*(RHEL)[-\\/]([\d\.]+)')


def update_filemap(fmap, dirname, fnames):
    for fn in fnames:
        fp = os.path.join(dirname, fn)
        if os.path.isfile(fp):
            md5 = hashlib.md5()
            md5.update(open(fp, 'rb').read())
            fmap[fp[1:]] = md5.hexdigest()


def parseRPM(path, platform, calculate_checksums=True):
    rpminfo = RPMInfo(path)
    arch = rpminfo.arch

    if arch != 'x86_64':
        arch = 'i386'  # no stupid i586 (XXX: what about noarch?)

    if platform:
        pem_platform = Platform(platform[0], platform[1], arch)
    else:
        pem_platform = Platform('any', 'any', 'any')

    rpm = RPM(rpminfo.name, pem_platform, os.path.basename(path))
    content = None

    return BuiltRPM(rpm, rpminfo.version, rpminfo.release, content)


def add_files(ctx, dirname, files):
    uLogging.debug("add_files called, dirname %s, cwd %s" % (dirname, os.getcwd()))
    build = ctx[0]
    files_filter = ctx[1]
    pmatch = platform_dir_pattern.match(dirname)
    if pmatch:
        guessed_platform = (pmatch.group(1), pmatch.group(2))
    else:
        guessed_platform = None

    if os.path.basename(dirname) == 'bin' and guessed_platform:
        if not guessed_platform in build.platform_binaries:
            build.platform_binaries[guessed_platform] = []
        build.platform_binaries[guessed_platform] += [os.path.join(dirname, x) for x in files]

    for fn in files:
        fullname = os.path.join(dirname, fn)  # name relative to distrib root, e g ./upgrade/update.udl2
        fullname = string.lstrip(fullname, './')
        fullname = string.lstrip(fullname, '\\')    # for unit-tests on windows
        if files_filter:
            if not os.path.isdir(fullname):
                fullname_norm = fullname.replace('\\', '/')     # for unit-tests on windows
                if fullname_norm not in files_filter:
                    uLogging.debug("filtered out: %s" % fullname_norm)
                    continue

        if os.path.islink(fullname) and os.path.isdir(fullname):
            uLogging.info("Walking down symlinked directory %s", fullname)
            os.path.walk(fullname, add_files, ctx)
        elif manifest_pattern.match(fn):
            try:
                pkg_info = get_package(fullname)
            except Exception, e:
                uLogging.err("%s while processing %s", e, fullname)
                raise
            pkg_info.manifest_file = fullname
            pkg_info.topdir = build.topdir
            build.add_package(pkg_info, build.fail_on_duplicates)
        elif tarball_pattern.match(fn):
            if not fn in build.tarballs_locations:
                build.tarballs_locations[fn] = []
            build.tarballs_locations[fn].append(dirname)
        elif fn == 'PAgent.exe':
            build.windows_update.sn_installer_path = fullname
            build.add_file(Const.getDistribWinDir(), fn, dirname)
        elif fn.lower() == 'asyncexec.exe':
            build.windows_update.async_exec_path = fullname
            build.add_file(Const.getDistribWinDir(), fn, dirname)
        elif rpm_pattern.match(fn):
            build.add_rpm(guessed_platform, os.path.abspath(fullname))
            if not guessed_platform:
                uLogging.info("Assuming any-any-any platform for %s", fullname)
            build.add_file(guessed_platform, fn, dirname)
        elif fn == 'update.udl2':
            build.udl2_file = os.path.abspath(fullname)
        elif exe_pattern.match(fn):
            build.add_file(Const.getDistribWinDir(), fn, dirname)
        elif mdl_pattern.match(fn):
            build.add_module(MDLReader.readModule(fullname))


def rmdir(dir):
    try:
        shutil.rmtree(dir)
    except:
        pass

def read_u_folder(build):
    if os.path.exists('u/pau/core-ear.ear'):
        build.add_pau(os.path.abspath('u/pau'))
    if os.path.exists('u/pui/pui-war.war'):
        build.add_pui(os.path.abspath('u/pui'))
    if os.path.exists('u/wildfly-dist.zip'):
        build.add_jboss_distribution(os.path.abspath('u'))


def getBuildFromFS(path, buildname, conf=None, fail_on_duplicates=False, calculateChecksums=True, filesFilter=None):
    """
    Returns instance of uBuild.Build class
    """
    rv = Build(buildname, conf)
    rv.fail_on_duplicates = fail_on_duplicates
    rv.calculate_checksums = calculateChecksums

    curdir = os.getcwd()
    rv.topdir = os.path.abspath(path)
    uLogging.debug('reading build from %s' % rv.topdir)
    try:
        os.chdir(path)
        os.path.walk(".", add_files, (rv, filesFilter))
        read_u_folder(rv)
    finally:
        os.chdir(curdir)
    uLogging.debug('read build: %s' % rv)
    return rv


def getBuildsFromFSOrCache(build_directories, config):
    config.update_name = '_'.join([os.path.basename(os.path.realpath(x))
                                   for x in [build_directories[0], build_directories[-1]]])

    if not config.cache:
        home = uUtil.findHome()
        default_cache_name = 'uc_' + config.update_name
        default_cache_dir = os.path.join(home, default_cache_name)
        config.cache = os.path.abspath(os.path.realpath(default_cache_dir))
    else:
        config.cache = os.path.abspath(os.path.realpath(config.cache))

    # TODO: Completely remove using build cache from updater
    rmdir(config.cache)
    os.makedirs(config.cache)
    builds = [getBuildFromFS(x, os.path.basename(os.path.realpath(x)), config, False, False)
              for x in build_directories]

    return builds


__all__ = ["getBuildFromFS"]
