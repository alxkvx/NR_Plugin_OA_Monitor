import threading
import os
import re
import sys
import shutil
import errno
import glob

import uSysDB
import uDBSchema
import uLogging
import uDialog
import uPEM
import uUtil
import uBuild
import uAction
import openapi
from uConst import Const

import subprocess as sp


def init(newest_packages, plesk_root):
    global _newest_packages, _mn_plesk_root, _ppm_ctl_cmd
    _newest_packages, _mn_plesk_root = newest_packages, plesk_root

_newest_packages = None
_mn_plesk_root = None

_ppm_ctl_cmd = None


def ppm_ctl_cmd():
    global _mn_plesk_root, _ppm_ctl_cmd

    if _ppm_ctl_cmd is None:
        if _mn_plesk_root is None:
            dummy, _mn_plesk_root = uPEM.getMNInfo()
        _ppm_ctl_cmd = [os.path.join(_mn_plesk_root, 'bin', 'ppm_ctl'), '-b', '-q', '-f',
                        os.path.join(_mn_plesk_root, 'etc', 'pleskd.props')]
    return _ppm_ctl_cmd


class PackageNotFound(Exception):

    def __init__(self, name, ctype, platform):
        Exception.__init__(self, "Package not found: %s-%s for %s" % (ctype, name, platform))


class ComponentNotFound(Exception):

    def __init__(self, name, ctype, host_id):
        Exception.__init__(self, "Component not found: %s-%s on host %s" % (ctype, name, host_id))


def getLatestPackages(name, ctype):
    if _newest_packages is None:
        # We're not initialized, it means that POST section is not in progress yet
        raise Exception, "Packages installation is allowed in POST phase only"
    rv = []
    for pf in _newest_packages:
        if _newest_packages[pf].get(ctype, {}).has_key(name):
            rv.append(_newest_packages[pf][ctype][name])
    return rv


def findSuitablePackage(host_id, name, ctype):
    con = uSysDB.connect()
    uSysDB.set_verbose(False)
    platform, rootpath = uPEM.getHostInfo(con, host_id)
    rv = findPackageByPlatform(con, platform, name, ctype)
    uSysDB.set_verbose(True)
    return rv


def findPackageByPlatform(con, platform, name, ctype):
    platforms = uPEM.getPlatformLine(con, platform)
    cur = con.cursor()
    if _mn_plesk_root is None:
        cur.execute("SELECT default_rootpath FROM hosts WHERE host_id = 1")
        init(None, cur.fetchone()[0])

    cur.execute(("SELECT p.pkg_id, p.version FROM packages p WHERE name = %%s AND ctype = %%s AND platform_id IN (%s)" % ','.join(
        ['%s'] * len(platforms))), [name, ctype] + [p.platform_id for p in platforms])
    pkgs = cur.fetchall()
    rv = None
    for p in pkgs:
        if rv is None or uBuild.compare_versions(p[1], rv[1]) > 0:
            rv = p
    if rv is None:
        raise PackageNotFound(name, ctype, platform)
    return rv


def findHostComponentId(host_id, name, ctype):
    """
    Returns component_id and version of the requested package on specified host
    :param host_id:
    :param name:
    :param ctype:
    :return: (component_id, version)
    :raise: ComponentNotFound if not such component
    """
    component = version = None
    try:
        pkg_id, version = findSuitablePackage(host_id, name, ctype)

        con = uSysDB.connect()
        cur = con.cursor()
        cur.execute(
            'SELECT c.component_id FROM packages p INNER JOIN components c ON p.pkg_id = c.pkg_id WHERE p.pkg_id = %s AND c.host_id = %s', (pkg_id, host_id))
        component = cur.fetchone()
    except PackageNotFound:
        pass

    if component is None:
        raise ComponentNotFound(name, ctype, host_id)

    return component[0], version


def installPackageToHost(host_id, name, ctype, properties={}):
    def props_to_list(props):
        return [("%s=%s" % (x, props[x])) for x in props]

    pkg_id, version = findSuitablePackage(host_id, name, ctype)
    uLogging.debug("Installing %s-%s-%s (%s) to host %s", ctype, name, version, pkg_id, host_id)
    properties_stripped = uUtil.stipPasswords(properties)
    cmd_common = ppm_ctl_cmd() + ["install", str(host_id), str(pkg_id), "INSTALL"]
    uUtil.execCommand(command=cmd_common + props_to_list(properties),
                      command_to_log=cmd_common + props_to_list(properties_stripped))


def reinstallPackageToHost(host_id, name, ctype):
    component_id, version = findHostComponentId(host_id, name, ctype)
    uLogging.debug("Reinstalling package (%s-%s-%s) to host %s", ctype, name, version, host_id)
    uPEM.execCtl("ppm_ctl", "-b", "-q", "reinstall", str(host_id), str(component_id))


def reinstallPackagesToHost(host_id, packages):
    for name, ctype in packages:
        reinstallPackageToHost(host_id, name, ctype)


def installPackageToHostAPI(host_id, name=None, ctype=None, pkg_id=None, properties=None, print_name=None):
    if pkg_id is None:
        pkg_id, version = findSuitablePackage(host_id, name, ctype)
    else:
        version = None

    if print_name is None:
        if name is None:
            print_name = 'package'
        elif ctype is None:
            print_name = name
        elif version is None:
            print_name = "%s-%s" % (ctype, name)
        else:
            print_name = "%s-%s-%s" % (ctype, name, version)
    uLogging.debug("installing %s (%s) to host %s", print_name, pkg_id, host_id)

    if properties is None:
        proplist = []
    else:
        proplist = [{"name": x, "value": properties[x]} for x in properties]

    # As DB schema is going to be changed during 'installPackageSync' call
    # we have to make sure that only one transaction will be exist to avoid deadlocks in Postgres (#POA-75494)
    con = uSysDB.connect()
    uSysDB.close(con)
    api = openapi.OpenAPI()
    return api.pem.packaging.installPackageSync(host_id=host_id, package_id=pkg_id, properties=proplist)["component_id"]


class PkgOnHost:

    def __init__(self, row):
        self.component_id, self.host_id, self.name, self.ctype, self.pkg_id, self.version, self.hostname = row[
            0], row[1], row[2], row[3], row[4], row[5], row[6]


def _getPkgsByQuery(q, *params):
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT c.component_id, c.host_id, p.name, p.ctype, p.pkg_id, p.version, h.primary_name " + q, *params)

    return [PkgOnHost(row) for row in cur.fetchall()]


def getDependencies(component_id):
    return _getPkgsByQuery("FROM component_dependencies d JOIN components c ON (c.component_id = d.dep_id) JOIN packages p ON (c.pkg_id = p.pkg_id) JOIN hosts h ON (h.host_id = c.host_id) WHERE d.component_id = %s", component_id)


def listInstalledPackages(name=None, ctype=None):
    """
    Returnt list of packages with specified name and component type.
    :param name: name of package
    :param ctype: type of package 'cp', 'sc', etc.
    :return: list of tuples (component_id, host_id, name, ctype, pkg_id, version, hostname)
    """
    return _getPkgsByQuery("FROM components c JOIN packages p ON (c.pkg_id = p.pkg_id) JOIN hosts h ON (c.host_id = h.host_id) WHERE p.name = %s AND p.ctype= %s", name, ctype)


def listInstalledPackagesOnHost(host_id):
    return _getPkgsByQuery("FROM packages p JOIN components c ON (c.pkg_id = p.pkg_id) JOIN hosts h ON (c.host_id = h.host_id) WHERE c.uninstalled = 'n' AND h.host_id = %s ORDER BY component_id", host_id)

def listAvailablePackages(host_id):
    con = uSysDB.connect()
    platform, rootpath = uPEM.getHostInfo(con, host_id)
    platforms = uPEM.getPlatformLine(con, platform)
    cur = con.cursor()
    cur.execute("SELECT p.pkg_id, p.name, p.ctype FROM packages p where platform_id IN (%s)" % ','.join(['%s'] * len(platforms)), [p.platform_id for p in platforms])
    return cur.fetchall()

def removeComponent(host_id, component_id, hostname="host", pkg_name="package", removeDepends=None):
    uAction.progress.do("removing %s(id = %s) from %s(id = %s)", pkg_name, component_id, hostname, host_id)

    deps = getDependencies(component_id)

    while deps:
        uLogging.info("%s-%s-%s (%s) installed on %s(%d) depends on %s (%s)", deps[0].ctype, deps[0].name, deps[
                      0].version, deps[0].component_id, deps[0].hostname, deps[0].host_id, pkg_name, component_id)
        if ((removeDepends is None) and uDialog.askYesNo("Shall I remove it also?", False)) or removeDepends:
            removeComponent(deps[0].host_id, deps[0].component_id, deps[0].hostname, deps[0].name, removeDepends)
        else:
            raise Exception("Cannot deinstall package, there are dependencies")
        deps = getDependencies(component_id)

    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute('SELECT pkg_id FROM components WHERE component_id = %s', (component_id))
    pkg_id = cur.fetchone()[0]
    api = openapi.OpenAPI()
    api.pem.packaging.uninstallPackageSync(host_id=host_id, package_id=pkg_id)
    uAction.progress.done()


def removePkgFromRepo(pkg_id, nicename='package'):
    uAction.progress.do("removing %s (%s) from repository", nicename, pkg_id)
    uUtil.execCommand(ppm_ctl_cmd() + ['remove', 'pkg', str(pkg_id)])
    uAction.progress.done()


def removePackage(name, ctype, host_id=None, removeDepends=None, platform=None):
    """ Removes package from host or from the whole system """
    con = uSysDB.connect()
    cur = con.cursor()

    nicename = "%s-%s" % (ctype, name)

    if _mn_plesk_root is None:
        cur.execute("SELECT default_rootpath FROM hosts WHERE host_id = 1")
        init(None, cur.fetchone()[0])

    if host_id is None:
        platform_id = None
        if platform:
            if hasattr(platform, "platform_id"):
                platform_id = platform.platform_id

            if platform_id is None:
                cur.execute("SELECT platform_id FROM platforms WHERE opsys = %s AND osrel = %s AND arch = %s",
                            (platform.os, platform.osver, platform.arch))
                row = cur.fetchone()
                if row:
                    platform_id = row[0]

        query = "SELECT h.host_id, h.primary_name, c.component_id, h.htype FROM packages p JOIN components c ON (c.pkg_id = p.pkg_id) JOIN hosts h ON (h.host_id = c.host_id) WHERE p.name = %s AND p.ctype = %s"
        params = (name, ctype)
        if platform_id:
            query += " AND h.platform_id = %s"
            params += (platform_id,)

        cur.execute(query, params)

        to_deinstall = [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]
        query = "SELECT pkg_id FROM packages WHERE name = %s AND ctype = %s"
        if platform_id:
            query += " AND platform_id = %s"

        cur.execute(query, params)
        to_remove = [row[0] for row in cur.fetchall()]
    else:
        cur.execute(
            "SELECT h.host_id, h.primary_name, c.component_id, h.htype FROM packages p JOIN components c ON (c.pkg_id = p.pkg_id) JOIN hosts h ON (h.host_id = c.host_id) WHERE p.name = %s AND p.ctype = %s AND h.host_id = %s", (name, ctype, host_id))

        to_deinstall = [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]
        to_remove = []

    con.commit()
    uSysDB.close(con)

    if not to_deinstall and host_id:
        uLogging.info("%s is not installed on host %s, doing nothing", (nicename, host_id))
        return
    elif not to_deinstall:
        uLogging.info("%s is not installed anywhere, doing nothing", nicename)
    else:
        for td in to_deinstall:
            host_id, hostname, component_id, host_type = td
            removeComponent(host_id, component_id, hostname, nicename, removeDepends)

    if not to_remove:
        if not host_id:
            uLogging.info("There is no %s in package repository, nothing to remove", nicename)
    else:
        for pkg_id in to_remove:
            removePkgFromRepo(pkg_id, nicename)


def registerPlatform(platform, parent, con=None):
    own_con = False
    if con is None:
        own_con = True
        con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM platforms "
        "WHERE arch = %s AND opsys = %s AND osrel = %s",
        (platform.arch, platform.os, platform.osver))
    if cur.fetchone():
        return

    cur.execute(
        "SELECT r, platform_id FROM platforms "
        "WHERE arch = %s AND opsys = %s AND osrel = %s",
        (parent.arch, parent.os, parent.osver))
    parent_platform = cur.fetchone()
    if not parent:
        raise Exception("Parent platform %s-%s-%s not found" % (parent.arch, parent.os, parent.osver))
    r = parent_platform[0]
    parent_id = parent_platform[1]

    cur.execute("UPDATE platforms SET l = l + 2 WHERE l > %s", r)
    cur.execute("UPDATE platforms SET r = r + 2 WHERE r >= %s", r)
    cur.execute("INSERT INTO platforms(arch, opsys, osrel, parent_id, l, r) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                platform.arch, platform.os, platform.osver, parent_id, r, r + 1)

    if own_con:
        con.commit()

__magic_value = 0xdeadbee  # should be less than max signed 32-bit int


def __rebuildSubPlatforms(con, parent_id):
    cur = con.cursor()

    cur.execute("SELECT platform_id FROM platforms "
                "WHERE parent_id = %s AND platform_id <> parent_id ORDER BY platform_id", parent_id)
    for row in cur.fetchall():
        platform_id = row[0]

        # re-fetch parent r (changed on every subplatform re-addition)
        cur.execute("SELECT r FROM platforms WHERE platform_id = %s", parent_id)
        row = cur.fetchone()
        if not row:
            raise Exception("Parent platform #%s not found" % (parent_id))
        r = row[0]

        # re-add subplatform
        cur.execute("UPDATE platforms SET l = l + 2 WHERE l <> %s AND l > %s", __magic_value, r)
        cur.execute("UPDATE platforms SET r = r + 2 WHERE r <> %s AND r >= %s", __magic_value, r)
        cur.execute("UPDATE platforms SET l = %s, r = %s WHERE platform_id = %s", r, r + 1, platform_id)
        __rebuildSubPlatforms(con, platform_id)


def getHostPlatform(con, host_id):
    cur = con.cursor()

    cur.execute("SELECT platform_id FROM hosts WHERE host_id = %s", host_id)
    row = cur.fetchone()
    if not row:
        raise Exception("No host with id = %s" % host_id)

    return row[0]


def getPlatformId(con, platform):
    cur = con.cursor()

    cur.execute("SELECT platform_id FROM platforms WHERE arch = %s AND opsys = %s AND osrel = %s", platform)
    row = cur.fetchone()
    if not row:
        raise Exception("%s-%s-%s - unknown platform" % platform)
    return row[0]


def rebuildPlatformTree(con):
    cur = con.cursor()

    # get any-any-any id
    cur.execute("SELECT platform_id FROM platforms "
                "WHERE arch = 'any' AND opsys = 'any' AND osrel = 'any'")
    row = cur.fetchone()
    if not row:
        raise Exception("Platform any-any-any not found")
    platform_id = row[0]

    # set l, r for any-any-any and rebuild subplatforms
    cur.execute("UPDATE platforms SET l = 1, r = 2 WHERE platform_id = %s", platform_id)
    cur.execute("UPDATE platforms SET l = %s, r = %s WHERE platform_id <> %s",
                __magic_value, __magic_value, platform_id)
    __rebuildSubPlatforms(con, platform_id)

    # check for dangling platforms (that are not children of any-any-any)
    cur.execute("SELECT platform_id, arch, opsys, osrel FROM platforms WHERE l = %s OR r = %s",
                __magic_value, __magic_value)
    for row in cur.fetchall():
        uLogging.warn("Dangling platform #%d (%s-%s-%s)", row[0], row[1], row[2], row[3])


def getNewestPackages(builds, filt=None):
    uLogging.debug("searching for latest packages to install")
    rv = {}
    for build in builds:
        for pkg in build.contents.values():
            platform = pkg.package.platform
            if not rv.has_key(platform):
                rv[platform] = {}
            ctype = pkg.package.ctype
            if not rv[platform].has_key(pkg.package.ctype):
                rv[platform][ctype] = {}
            name = pkg.package.name
            if not (rv[platform][ctype].has_key(pkg.package.name)) or (uBuild.compare_versions(rv[platform][ctype][name].version, pkg.version) <= 0):
                pkg.tarball_location = build.find_valid_tarball(pkg)
                if pkg.tarball_location:
                    pkg.tarball_location = os.path.realpath(pkg.tarball_location)
                if filt is None or (pkg.package.ctype, pkg.package.name) in filt:
                    rv[platform][ctype][name] = pkg
    return rv

mass_import_out_pattern = re.compile("^([one]) (\\d+)\n?\r?$")


def __assignPkgIds(output, pkglist):
    i = 0
    line = output.readline()
    while line:
        if i < len(pkglist):
            pkg = pkglist[i]
            outp = mass_import_out_pattern.match(line)
            if not outp:
                uLogging.err("Cannot understand ppm_ctl output: '%s'", line)
            oldnew, pkg_id = outp.group(1), outp.group(2)
            pkg.pkg_id = int(pkg_id)
            if oldnew == 'n':
                pkg.old = False
            elif oldnew == 'o':
                pkg.old = True
            else:
                pkg.old = None
        i += 1
        line = output.readline()


def __showProgress(output, pkg_counter, full_output):
    line = output.readline()
    while line:
        if line.endswith('\n'):
            line = line.replace('\n', '')
        if line.endswith('.pdl.asc'):
            line = line.replace('.pdl.asc', '')
        pkg_counter.new_item(os.path.basename(line))
        full_output.append(line)
        line = output.readline()


def __packageImported(pkg):
    """Return True if package with specific name, ctype, and platform exists in POA database. Version is ignored."""
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM dual WHERE EXISTS (SELECT 1 FROM packages pk JOIN platforms pl ON (pk.platform_id = pl.platform_id) WHERE pk.name = %s AND pk.ctype = %s AND pl.arch = %s AND pl.opsys = %s AND pl.osrel = %s)",
                pkg.name, pkg.ctype, pkg.platform.arch, pkg.platform.os, pkg.platform.osver)
    return bool(cur.fetchall())

### TODO: Terminate after saas_ctl upgrade fix
def __filterCustomPackages(pkglist):
    """Filters out customer specific packages that were not imported before."""
    return [p for p in pkglist if not p.package.is_custom or __packageImported(p.package)]

### TODO: Terminate after saas_ctl upgrade fix
def doMassImport(rootpath, pkglist, pkg_counter):
    command = [os.path.join(rootpath, "bin", "ppm_ctl"), '-q', '-b', '-E', '-f',
               os.path.join(rootpath, "etc", "pleskd.props"), 'add', 'pkgmass']
    pkglist = __filterCustomPackages(pkglist)

    if not pkglist:
        return

    command += [os.path.join(x.topdir, x.manifest_file) for x in pkglist]

    uLogging.debug("doMassImport: %s", command)

    if Const.isWindows():
        p = sp.Popen(command, bufsize=1, stdout=sp.PIPE, stderr=sp.PIPE, startupinfo=uUtil.startup_info)
    else:
        p = sp.Popen(command, bufsize=1, stdout=sp.PIPE, stderr=sp.PIPE)

    out_thread = threading.Thread(target=__assignPkgIds, args=(p.stdout, pkglist))
    out_thread.setDaemon(True)
    errlines = []
    err_thread = threading.Thread(target=__showProgress, args=(p.stderr, pkg_counter, errlines))
    err_thread.setDaemon(True)
    out_thread.start()
    err_thread.start()
    out_thread.join()
    err_thread.join()
    status = p.wait()

    if status > 0:
        raise Exception('ppm ctl exited with code %d: %s' % (status, "\n".join(errlines)))
    elif status < 0:
        raise Exception('ppm ctl terminated with signal %d' % -status)

### TODO: Terminate after saas_ctl upgrade fix
### TODO: Don't forget to revert ypack-ejb/../Package.java L2 cache off after terminations
def updateManifestSources(plesk_root, pkglist, pkg_counter):
    uAction.progress.do("updating agent manifests")
    con = uSysDB.connect()
# Update old package set its version to zero (zero is less than anything else :)
# After it, import new package, update components and properties to point to new version, and delete old one.
    for pkg in pkglist:
        uAction.progress.do("updating %s", pkg)
        old_pkg_id = pkg.pkg_id
        cur = con.cursor()
        cur.execute("UPDATE packages SET version = '0', filename = NULL WHERE pkg_id = %s", old_pkg_id)
        con.commit()

        uAction.retriable(doMassImport)(plesk_root, [pkg], pkg_counter)
        cur.execute("UPDATE components SET pkg_id = %s WHERE pkg_id = %s", pkg.pkg_id, old_pkg_id)

        cur.execute("""
		   SELECT p.prop_id, p.name, p2.prop_id AS new_prop_id,
		   cp.component_id FROM component_properties cp
		   JOIN properties p ON (cp.prop_id = p.prop_id)
		   LEFT JOIN properties p2 ON (p.name = p2.name)
		   WHERE p.pkg_id = %s
		   AND p2.pkg_id = %s """, (old_pkg_id, pkg.pkg_id))

        rows = cur.fetchall()

        for row in rows:
            old_prop_id, prop_name, new_prop_id, component_id = row[0], row[1], row[2], row[3]
            if new_prop_id is None:
                uLogging.debug("Deleting property %s (old prop_id %s)", prop_name, old_prop_id)
                cur.execute("""DELETE FROM component_properties
		                    WHERE component_id = %s AND prop_id = %s """, (component_id, old_prop_id))
            else:
                uLogging.debug('Copying property %s (old prop_id %s, new prop_id %s)',
                               prop_name, old_prop_id, new_prop_id)
                cur.execute("""UPDATE component_properties
				    SET prop_id = %s
		                    WHERE component_id = %s
		                    AND prop_id = %s """, (new_prop_id, component_id, old_prop_id))
        cur.execute("DELETE FROM packages WHERE pkg_id = %s", old_pkg_id)
        con.commit()
        uAction.progress.done()
        pkg_counter.new_item(pkg)
    uAction.progress.done()


def importPackages(newestPackages, plesk_root, pkg_counter=None):
    # import pleskds - it registers platforms in system.
    uLogging.info("Importing new packages")
    new_pleskds = []

    total_packages = 0
    for pform_ctypes in newestPackages.values():
        for ctype_packages in pform_ctypes.values():
            total_packages += len(ctype_packages)

    if not pkg_counter:
        pkg_counter = uUtil.CounterCallback()

    pkg_counter.set_total(total_packages)

    for pform in newestPackages:
        if newestPackages[pform].has_key('other') and newestPackages[pform]['other'].has_key('pleskd'):
            new_pleskds.append(newestPackages[pform]['other']['pleskd'])

    if new_pleskds:
        uLogging.debug("importing new agent packages")
        uAction.retriable(doMassImport)(plesk_root, new_pleskds, pkg_counter)
        to_update = [x for x in new_pleskds if x.old]
        if to_update:
            updateManifestSources(plesk_root, to_update, pkg_counter)

    for pform in newestPackages:
        uLogging.debug("importing new packages for %s", pform)
        for ctype in newestPackages[pform]:
            if newestPackages[pform][ctype]:
                uAction.retriable(doMassImport)(plesk_root, newestPackages[pform][ctype].values(), pkg_counter)


def updatePPMs(newest_packages):
    mirror = getMainMirror()
    host, path, host_id = mirror.hostname, mirror.localpath, mirror.host_id

    tarballs_to_copy = []
    for pform in newest_packages:
        for ctype in newest_packages[pform]:
            tarballs_to_copy += [x.tarball_location for x in newest_packages[pform]
                                 [ctype].values() if x.tarball_location]

    uAction.progress.do("updating packages at mirror '%s'", host)
    if host_id == 1:
        for fn in tarballs_to_copy:
            uUtil.ln_cp(fn, path)
            uLogging.debug("Copying %s to %s", fn, path)
    else:
        raise Exception("updateOneMirror is not implemented for host_id != 1")

    uAction.progress.done()


def getLatestPackageId(pform_id, pkg):
    """
    Returns pkg_id for the latest version of package pkg for the specified platform
    :param platform_id: id of target platform
    :param pkg: a (ctype, name) structure
    :return: pkg_id
    """
    con = uSysDB.connect()
    cur = con.cursor()
    ctype, name = pkg
    cur.execute(
        "SELECT pkg_id, version FROM packages WHERE platform_id = %s AND ctype = %s AND name = %s", pform_id, ctype, name)
    rv = None
    for row in cur.fetchall():
        pkg_id, version = row[0], row[1]
        if rv is None or uBuild.compare_versions(version, rv[1]) > 0:
            rv = pkg_id, version

    if rv is not None or pform_id == 1:
        return rv
    cur.execute("SELECT parent_id FROM platforms WHERE platform_id = %s", pform_id)
    return getLatestPackageId(cur.fetchone()[0], pkg)


def deployBaseRpmFiles(source_path, local_path, packages):
    if not local_path.endswith('/'):
        local_path += '/'
    try:
        os.makedirs(local_path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
    for package in packages:
        src_files = glob.glob(os.path.join(source_path, "%s-*.rpm" % package))
        if src_files:
            src_file = src_files[0]
            src_file = os.path.abspath(os.path.realpath(src_file))
            # remove previous version of this rpm
            for dst_file in glob.glob(os.path.join(local_path, "%s-*.rpm" % package)):
                dst_file = os.path.join(local_path, dst_file)
                uLogging.debug("removing previous version rpm %s" % dst_file)
                os.remove(os.path.join(local_path, dst_file))
            uLogging.debug("Copying %s to %s" % (src_file, local_path))
            shutil.copy2(src_file, local_path)


def schedulePackagesUpgrade(packages, api, depends=None):
    if depends is None:
        depends = []
    to_install = []
    for pkg in packages:
        hosts = getPackageHosts(pkg)
        for pform_id in hosts:
            pkg_id = getLatestPackageId(pform_id, pkg)
            pform = uPEM.getPlatform(uSysDB.connect(), pform_id)
            if pkg_id is None:
                uLogging.err("%s: cannot find package for platform %s", pkg, pform)
                continue
            pkg_id, version = pkg_id
            uLogging.debug("Going to install %s v %s (%s) at %s", pkg, version, pkg_id, pform)
            to_install += [{'host_id': host_id, 'pkg_id': pkg_id} for host_id in hosts[pform_id]]

    return api.pem.packaging.asyncUpgrade(component_list=to_install, depend_on=depends)["task_ids"]


def scheduleOrderedUpgrade(packages):
    task_ids = []
    api = openapi.OpenAPI()
    api.begin()
    for line in packages:
        task_ids = schedulePackagesUpgrade(line, api, task_ids)
    api.commit()


def getPackageHosts(pkg):
    ctype, name = pkg
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "SELECT h.platform_id, h.host_id FROM hosts h JOIN components c ON (c.host_id = h.host_id) JOIN packages p ON (p.pkg_id = c.pkg_id) WHERE p.name = %s AND p.ctype = %s", name, ctype)
    rv = {}
    for row in cur.fetchall():
        pform_id = row[0]
        host_id = row[1]
        if rv.has_key(pform_id):
            rv[pform_id].append(host_id)
        else:
            rv[pform_id] = [host_id]

    return rv


class PPMMirror:

    def __init__(self, mirror_id, hostname, localpath, host_id):
        self.id = mirror_id
        self.hostname = hostname
        self.localpath = localpath
        self.host_id = host_id


def getMainMirror():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT h.primary_name, h.default_rootpath FROM hosts h WHERE h.host_id = 1")
    primary_name, default_rootpath, = cur.fetchone()
    mainMirror = PPMMirror(1, primary_name, os.path.join(default_rootpath, "install", "tarballs"), 1)
    return mainMirror


def clean_interfaces():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "DELETE FROM interfaces WHERE NOT EXISTS (SELECT 1 FROM package_interfaces WHERE package_interfaces.interface_id = interfaces.interface_id UNION ALL SELECT 1 FROM dep_interfaces di WHERE di.interface_id = interfaces.interface_id)")
    con.commit()
    cur.execute(
        "SELECT p.pkg_id, p.name, p.ctype, i.service_type FROM interfaces i JOIN dep_interfaces di ON (i.interface_id = di.interface_id) JOIN package_dependencies dp ON (di.dep_id = dp.dep_id) JOIN packages p ON (dp.pkg_id = p.pkg_id) WHERE i.interface_id NOT IN (SELECT interface_id FROM package_interfaces) AND p.ctype != 'cp'")
    pkgs = {}
    for row in cur.fetchall():
        pkg_name = row[1], row[2]
        if not pkgs.has_key(pkg_name):
            pkgs[pkg_name] = [], []
        pkg_id = str(row[0])
        iface = row[3]
        if pkg_id not in pkgs[pkg_name][0]:
            pkgs[pkg_name][0].append(pkg_id)
        if iface not in pkgs[pkg_name][1]:
            pkgs[pkg_name][1].append(iface)

    for pkg in pkgs:
        uLogging.warn("%s-%s (%s) depends on non-existing interface(s): %s",
                      pkg[0], pkg[1], ', '.join(pkgs[pkg][0]), ', '.join(pkgs[pkg][1]))

tarball_pattern = re.compile(r'.+\.(tar\.gz|tgz)$')
pagent_pattern = re.compile(r'PAgent.exe$')


def copy_tarballs(to, dirname, fnames):
    for fn in fnames:
        if tarball_pattern.match(fn):
            uUtil.ln_cp(os.path.join(dirname, fn), to)
        elif pagent_pattern.match(fn):
            update_install_win_sn(os.path.join(dirname, fn), to)


def updateComponentPackage(con, component_id, name, ctype, new_platform_id, old_pkg_id):
    new_platform = uPEM.getPlatform(con, new_platform_id)

    cur = con.cursor()
    try:
        pkg_id, ver = findPackageByPlatform(con, new_platform, name, ctype)

        cur.execute("UPDATE components SET pkg_id = %s WHERE component_id = %s", pkg_id, component_id)
        cur.execute("""
		   SELECT p.prop_id, p.name, p2.prop_id AS new_prop_id
		   FROM component_properties cp
		   JOIN properties p ON (cp.prop_id = p.prop_id)
		   LEFT JOIN properties p2 ON (p.name = p2.name)
		   WHERE p.pkg_id = %s
		   AND p2.pkg_id = %s
		   AND cp.component_id = %s""", (old_pkg_id, pkg_id, component_id))

        rows = cur.fetchall()
        for row in rows:
            old_prop_id, prop_name, new_prop_id = row[0], row[1], row[2]
            if new_prop_id is None:
                uLogging.debug("Deleting property %s (old prop_id %s)", prop_name, old_prop_id)
                cur.execute(
                    """DELETE FROM component_properties WHERE component_id = %s AND prop_id = %s """, (component_id, old_prop_id))
            else:
                uLogging.debug('Copying property %s (old prop_id %s, new prop_id %s)',
                               prop_name, old_prop_id, new_prop_id)
                cur.execute("""UPDATE component_properties SET prop_id = %s WHERE component_id = %s AND prop_id = %s """,
                            (new_prop_id, component_id, old_prop_id))
        return True
    except PackageNotFound:
        return False


def updatePackagesRecords(con, host_id, old_platform_id, new_platform_id):
    old_platform = uPEM.getPlatform(con, old_platform_id)
    platforms = uPEM.getPlatformLine(con, old_platform)

    cur = con.cursor()
    cur.execute(("SELECT p.pkg_id, c.component_id, p.name, p.ctype FROM packages p JOIN components c ON c.pkg_id = p.pkg_id WHERE c.host_id = %%s AND p.platform_id IN (%s)" %
                 ','.join(['%s'] * len(platforms))), [host_id] + [p.platform_id for p in platforms])
    for row in cur.fetchall():
        uLogging.info("Update DB records for package %s" % row[2])
        updateComponentPackage(con, row[1], row[2], row[3], new_platform_id, row[0])


def update_stored_installer(builds, plesk_root):
    dest_dir = os.path.join(plesk_root, 'bin')
    for build in builds:
        files = []
        files.append(os.path.join(build.topdir, 'install_routines.py'))
        files.append(os.path.join(build.topdir, 'deployment.py'))

    for fname in files:
        if os.path.exists(fname):
            shutil.copy(fname, dest_dir)
        else:
            raise Exception("Installation file does not exists: " + fname)


def update_install_win_sn(sn_installer_path, main_ppm_mirror_path):
    # copy PAgent.exe to PPM mirror for later use by SharedNodeRegistrator
    registrar_path = os.path.join(main_ppm_mirror_path, 'Win32Bin')
    if not os.path.exists(registrar_path):
        os.makedirs(registrar_path)
        os.chmod(registrar_path, 0775)
        if not Const.isWindows():
            import grp
            grpinfo = grp.getgrnam('pemgroup')
            os.chown(registrar_path, 0, grpinfo.gr_gid)
    if sn_installer_path:
        shutil.copy(sn_installer_path, registrar_path)


def pkg_installed(host_id, what):
    """
    Checks if a certain package specified by what param is installed on host with id host_id
    :param host_id: id of host where package is searched for
    :param what: a (type, name) structure (type - component type, and name - name of the package to search for)
    :return: True if database contains record about this package on specified host, False - otherwise
    """
    con = uSysDB.connect()
    cur = con.cursor()
    typ, nam = what
    cur.execute(
        "SELECT 1 FROM components c JOIN packages p ON (c.pkg_id = p.pkg_id) WHERE c.host_id = %s AND p.name=%s AND p.ctype=%s", (host_id, nam, typ))
    return bool(cur.fetchone())


def getInstalledSCs(con):
    """
    Gets a list of installed SCs
    :param con: an open database connection
    :return: list of items (name, version, rootpath)
    """
    cur = con.cursor()
    # we must take into account only SCs from MN (host_id = 1)
    cur.execute(
        "SELECT p.name, p.version, c.rootpath FROM packages p JOIN components c ON (c.pkg_id = p.pkg_id) WHERE p.ctype = 'sc' AND c.host_id = 1")
    return [(x[0], x[1], x[2]) for x in cur.fetchall()]


def getInstalledLibs(con):
    cur = con.cursor()
    cur.execute(
        "SELECT p.name FROM packages p JOIN components c ON (c.pkg_id = p.pkg_id) WHERE c.host_id = 1 AND p.ctype = 'other' AND p.name like 'lib%'")

    return [x[0] for x in cur.fetchall()]


def getPackageAttributes(pkg_id):
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT name, value FROM package_attributes WHERE pkg_id = %s", pkg_id)
    return uUtil.readPropertiesResultSet(cur.fetchall())

