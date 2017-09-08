try:
    import hashlib
except ImportError:
    import md5 as hashlib

import os.path
import re
from xml.dom import minidom as dom
import PEMVersion
from distutils.version import LooseVersion
import uLogging
import uAction
from uRPMUtils import RPMInfo


def compare_versions(version_one, version_two):
    return LooseVersion(str(version_one)).__cmp__(str(version_two))


class Platform:

    def __init__(self, os, osver, arch='i386'):
        self.os = os
        self.osver = osver
        self.arch = arch

    def __str__(self):
        return "%s-%s-%s" % (self.os, self.osver, self.arch)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.os == other.os and self.osver == other.osver and self.arch == other.arch

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(str(self))


# assuming RHEL/CentOS Linux distribution
def _create_platform(distrib_name, version, arch):
    if distrib_name.lower() in ('red hat enterprise linux server', 'redhat', 'centos', 'centos linux'):
        distrib_name = 'RHEL'
    else:
        raise Exception('unsupported distribution %s' % distrib_name)
    arch = arch.lower()
    if arch.lower() not in ('x86_64', 'i686'):
        raise Exception('unsupported architecture %s' % arch)
    version_arr = tuple(version.split('.'))
    p = Platform(distrib_name, version_arr[0], arch)
    p.osverfull = version_arr
    return p


def is_platform_compat(p1, p2):
    if p1 is None or p2 is None or p1 == p2:
        return True

    if p1.osver == p2.osver:
        return p1.os == 'CentOS' and p2.os.startswith('RHE') or \
            p1.os.startswith('RHE') and p2.os == 'CentOS' or \
            p1.os == p2.os and p1.arch == 'i386'


class Package:

    def __init__(self, name, ctype, platform, is_single=True, is_custom=False):
        self.name = name
        self.ctype = ctype
        self.platform = platform
        self.is_single = is_single
        self.is_custom = is_custom

    def __str__(self):
        return "%s-%s-%s" % (self.ctype, self.platform, self.name)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.name == other.name and self.ctype == other.ctype and self.platform == other.platform

    def __hash__(self):
        return hash(str(self))


class RPM:

    def __init__(self, name, platform, filename=None):
        self.name = name
        self.platform = platform
        self.filename = filename

    def __str__(self):
        return "%s-%s" % (self.platform, self.name)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.name == other.name and self.platform == other.platform

    def __hash__(self):
        return hash(str(self))


class BuiltRPM:

    def __init__(self, rpm, version, release, content):
        self.rpm = rpm
        self.version = version
        self.release = release
        self.content = content

    def __str__(self):
        if self.content is not None:
            return self.content.filename
        else:
            return str(self.rpm)

    def __repr__(self):
        return str(self)


class BuiltPackage:

    def __init__(self, package, version, content=None):
        self.package = package
        self.version = version
        self.content = content
        self.topdir = None
        self.old = None
        self.pkg_id = None
        self.build_tarball = None, None

    def __str__(self):
        return "%s-%s" % (self.package, self.version)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.package == other.package and self.version == other.version and (self.content is None and other.content is None or self.content.files == other.content.files)

    def __hash__(self):
        return hash(str(self))

    def findTarball(self, build):
        if self.build_tarball[0] != build:
            self.build_tarball = build, build.find_valid_tarball(self)
        return self.build_tarball[1]


class BuiltPackageContent:

    def __init__(self, filename, checksum, files):
        self.filename = filename
        self.checksum = checksum
        self.files = files


class WindowsUpdate:

    def __init__(self):
        self.mn_installer_path = None       # pa-core.msi
        self.sn_installer_path = None       # PAgent.exe
        self.async_exec_path = None         # AsyncExec.exe
        self.perform = False


def describe_difference(old, new):
    newfiles = set(new.iterkeys())
    oldfiles = set(old.iterkeys())
    added = list(newfiles - oldfiles)
    removed = list(oldfiles - newfiles)
    changed = [f for f in newfiles & oldfiles if old[f] != new[f]]

    return '\n'.join(["%s file(s): %s" % (kind, ", ".join(lst)) for kind, lst in [("Added", added), ("Removed", removed), ("Changed", changed)] if lst])


def get_ppm_versioning(build_name):
    product, version = PEMVersion.parseProductVersion(build_name)
    major_version = re.split("\\s", version)[0]
    ppm_rx = re.compile(re.sub("\\.", "\.", major_version) + "\\.(\\d+)\\.(\\d+)")
    return major_version, ppm_rx

class JBossDistrib:
    def __init__(self):
        self.pau = None     # abs path to pau folder. Filled if dist/u/pau is present
        self.pui = None     # abs path to pui folder. Filled if dist/u/pau is present
        self.distribution = None  # abs path to JBoss distribution folder. Filled if JBoss dist is present in dist/u

    def __str__(self):
        return 'JBoss [pau %s, pui %s, distribution %s' % (self.pau, self.pui, self.distribution)

class Build:

    """ Represents build characteristics """
    def __init__(self, name, conf=None):
        """ Creates new empty Build with name """
        self.name = name
        self.platforms = set()
        self.contents = {}
        self.config = conf
        self.rpms = {}      # ("poaupdater", "RHEL", 5) => {"info": PRMInfo, "path": FULL_PATH_TO_RPM}
        self.topdir = None
        self.tarballs_locations = {}
        self.platform_binaries = {}
        self.windows_update = WindowsUpdate()       # required for windows slaves update
        self.udl2_file = None
        self.modules = []
        self.files = {}
        self.jboss = JBossDistrib()

    def __str__(self):
        return 'Build [%s, %s, %s' % (self.name, self.udl2_file, self.jboss)

    def set_udl2(self, udl2_file):
        self.udl2_file = udl2_file

    def add_pau(self, pau_dir):
        self.jboss.pau = pau_dir

    def add_pui(self, pui_dir):
        self.jboss.pui = pui_dir

    def add_jboss_distribution(self, jboss_distribution):
        self.jboss.distribution = jboss_distribution

    def add_module(self, module):
        self.modules.append(module)

    def __add_package(self, built_package):
        self.contents[built_package.package] = built_package

    def add_package(self, built_package, fail_on_different_versions=False):
        self.platforms.add(built_package.package.platform)
        if self.contents.has_key(built_package.package):
            if self.config and self.config.warning_duplicates:
                uLogging.warn("%s: duplicate package in build %s", built_package, self.name)
            vc = compare_versions(built_package.version, self.contents[built_package.package].version)
            if vc != 0 and fail_on_different_versions:
                raise Exception("Two different package versions in build: %s and %s" %
                                (built_package, self.contents[built_package.package].version))

            if vc > 0:
                uLogging.debug("Choosing %s over %s", built_package, self.contents[built_package.package].version)
                self.__add_package(built_package)
            elif vc < 0:
                uLogging.debug("Choosing %s over %s", self.contents[built_package.package], built_package.version)
        else:
            self.__add_package(built_package)

    def add_file(self, platform, name, path):
        if not self.files.has_key(name):
            self.files[name] = []

        self.files[name].append((platform, path))

    def update_platforms(self):
        """ Make sure new attributes of platform go to packages """
        for platform in self.platforms:
            for package in self.contents.keys():
                if package.platform == platform:
                    package.platform = platform

            for package in self.rpms.keys():
                if package.platform == platform:
                    package.platform = platform

    def update_packages(self):
        """ Make sure new attributes of packages go to built packages """
        for package in self.contents.keys():
            for bp in self.contents.values():
                if bp.package == package:
                    bp.package = package

    def add_rpm(self, platform_set, path_to_rpm):
        full_path = path_to_rpm
        if path_to_rpm.startswith("."):
            full_path = os.path.abspath(path_to_rpm)
        rpm_info = RPMInfo(full_path)
        p_list = list(platform_set)
        p_list.insert(0, rpm_info.name)
        if self.rpms.get(tuple(p_list)) is not None:
            raise Exception("duplicate RPM in distrib, {0} ".format(",".join(p_list)))
        self.rpms[tuple(p_list)] = {"info": rpm_info, "path": full_path}

    def find_valid_tarball(self, package):
        if not package.content:
            return None
        if not self.tarballs_locations.has_key(package.content.filename):
            return None
        locations = self.tarballs_locations[package.content.filename]
        for i, tbd in enumerate(locations):
            if self.topdir:
                tb = os.path.join(self.topdir, tbd, package.content.filename)
            else:
                tb = os.path.join(tbd, package.content.filename)
            if i == (len(locations) - 1):
                return tb
            md5 = hashlib.md5()
            md5.update(open(tb, 'rb').read())
            checksum = md5.hexdigest()
            if checksum == package.content.checksum:
                return tb

        return None

    def get_news(self, other):
        """ Difference between other and this. This - considered newer """
        uAction.progress.do('calculating PPM difference between %s and %s' % (self.name, other.name))
        major_version_new, ppm_rx_new = get_ppm_versioning(self.name)
        if other.name != "Nobuild":		# build precheck - no old build given. TODO don't call get_news
            major_version_old, ppm_rx_old = get_ppm_versioning(other.name)

        new_platforms = []
        for platform in self.platforms:
            if platform not in other.platforms:
                new_platforms += [platform]

        new_packages = []
        new_versions = []
        different_contents = []
        for pkg in self.contents:
            if not other.contents.has_key(pkg):
                new_packages.append(self.contents[pkg])
            else:
                newv = self.contents[pkg].version
                oldv = other.contents[pkg].version
                uLogging.debug('package %s, newv %s, oldv %s' % (pkg, newv, oldv))
                if oldv != newv:
                    newm = ppm_rx_new.match(newv)
                    oldm = ppm_rx_old.match(oldv)
                    if oldm and newm:
                        # regular-versioned ppm: <POA major version>.<POA update version>.<package
                        # minor version> - ignore update version as it's auto-bumped on every
                        # update
                        if major_version_new != major_version_old or oldm.group(2) != newm.group(2):
                            new_versions.append(self.contents[pkg])
                            uLogging.debug('taken for update')
                    else:
                        new_versions.append(self.contents[pkg])
                        uLogging.debug('taken for update')
        uAction.progress.done()
        return new_platforms, new_packages, new_versions, different_contents

    def get_new_rpms(self, other):
        uAction.progress.do('calculating RPM difference between %s and %s' % (self.name, other.name))
        newRpms = []
        newVersions = []
        for rpm in self.rpms:
            newRpm = self.rpms[rpm]["info"]
            if other.rpms.get(rpm):
                oldRpm = other.rpms[rpm]["info"]
                uLogging.debug("RPM %s-%s-%s, newv %s-%s-%s, oldv %s-%s-%s" % (newRpm.name, rpm[1], rpm[2], newRpm.version, newRpm.release, newRpm.arch, oldRpm.version, oldRpm.release, oldRpm.arch))
                if newRpm > oldRpm:
                    newVersions.append(self.rpms[rpm])
                    uLogging.debug("taken for update")
            else:
                uLogging.debug("New RPM %s-%s-%s, newv %s-%s-%s" % (newRpm.name, rpm[1], rpm[2], newRpm.version, newRpm.release, newRpm.arch))
                oldRpm = None
                newRpms.append(self.rpms[rpm])
                uLogging.debug("taken for update")
        uAction.progress.done()
        return newRpms, newVersions
