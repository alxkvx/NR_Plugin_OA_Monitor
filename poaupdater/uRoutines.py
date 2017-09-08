import sys
import os
import tarfile
import shutil
import time
import errno
import re

import uLogging
import uPEM
import uSysDB
import uFakePackaging
import openapi
import uAction
import uUtil
import uTasks
import uBuild
import uHCL
import uPackaging
from uConst import Const

class PkgCounter(uUtil.CounterCallback):

    def __init__(self):
        uUtil.CounterCallback.__init__(self)

    def new_item(self, name):
        uLogging.debug("imported: %s", name)


def update_binary(pkg, rootpath):
    def do_unpack(pkg, rootpath):
        uLogging.info("Updating %s (%s)", pkg.package, pkg.tarball_location)
        arc = tarfile.open(pkg.tarball_location, 'r')
        for tarinfo in arc:
            arc.extract(tarinfo, path=rootpath)
        arc.close()
    if pkg.tarball_location is not None:
        try:
            do_unpack(pkg, rootpath)
        except IOError, e:
            if Const.isWindows() and e.errno == errno.EACCES:
                uLogging.err("Cannot unpack, file is probably locked. retrying")
                time.sleep(1)
                do_unpack(pkg, rootpath)
            else:
                raise


def update_binaries(packages, rootpath):
    for pkg, sc_rootpath in packages.values():
        if not sc_rootpath:
            sc_rootpath = rootpath
        uAction.retriable(update_binary)(pkg, sc_rootpath)


def updateScripts(mn_platform, plesk_root, ordered_builds):
    uAction.progress.do("updating scripts")
    opsys = mn_platform.os, mn_platform.osver
    for build in ordered_builds:
        if build.platform_binaries.has_key(opsys):
            for script in build.platform_binaries[opsys]:
                if not os.path.basename(script) in ['pem']:
                    shutil.copy2(os.path.join(build.topdir, script), os.path.join(plesk_root, 'bin'))
    uAction.progress.done()


def getPkgsToInstall(con, host_id, upgrade_instructions, newestPackages):
    cur = con.cursor()

    cur.execute(
        "SELECT p.name, p.ctype, p.pkg_id, p.version FROM packages p JOIN components c ON (c.pkg_id = p.pkg_id) WHERE c.host_id = %s ORDER BY c.component_id", host_id)
    installed_packages = {}
    installed_packages_sorted = []	# For storing order of installed packages
    installed_pkg_ids = set()
    for row in cur.fetchall():
        installed_packages[(row[0], row[1])] = row[3]
        installed_packages_sorted.append((row[0], row[1]))
        installed_pkg_ids.update([row[2]])

    to_install = []

    unicode_regexp = re.compile("(u'|')")
    if upgrade_instructions is not None:
        to_install_set = set()
        for pkg in upgrade_instructions.packages:
            # do not attempt to upgrade scs:
            name, ctype = pkg
            if (ctype == 'sc') and pkg in installed_packages:
                continue
            if pkg in upgrade_instructions.disabled_packages:
                uLogging.info('Not auto-installing %s-%s, it is disabled', ctype, name)
                continue
            for where in upgrade_instructions.packages[pkg]:
                if where and where in installed_packages:
                    uLogging.debug(unicode_regexp.sub("", 'Going to upgrade %s because %s is installed' % (pkg, where)))
                    to_install_set.update([pkg])
        # Create list of packages to install in correct order
        # First, add all already installed packages
        for pkg in installed_packages_sorted:
            if pkg in to_install_set:
                to_install.append(pkg)
        # Second, add new packages
        for pkg in to_install_set:
            if pkg not in installed_packages:
                to_install.append(pkg)
    else:
        to_install = [x for x in installed_packages_sorted]

    platform, rootpath = uPEM.getHostInfo(con, host_id)
    platforms_list = uPEM.getPlatformLine(con, platform)
    non_single = []
    single = []
    libs = []
    for pkg in to_install:
        name, ctype = pkg

        found = False
        for ok_pl in platforms_list:
            if newestPackages.has_key(ok_pl) and newestPackages[ok_pl].has_key(ctype) and newestPackages[ok_pl][ctype].has_key(name):
                found = True
                package = newestPackages[ok_pl][ctype][name]
                if hasattr(package, "pkg_id"):
                    name = package.package.name
                    ctype = package.package.ctype
                    if installed_packages.has_key((name, ctype)):
                        installed_version = installed_packages[(name, ctype)]
                        vc = uBuild.compare_versions(installed_version, package.version)
                        if vc >= 0:
                            uLogging.info("%s version %s of %s-%s is already installed",
                                          vc and 'Higher' or 'Same', installed_version, ctype, name)
                            continue

                    if ctype == 'other' and name.startswith('lib'):
                        libs.append(package)
                    elif package.package.is_single:
                        single.append(package)
                    else:
                        non_single.append(package)
                else:
                    uLogging.err("No pkg id for package %s", package)
                break
        if not found and upgrade_instructions:
            uLogging.warn(unicode_regexp.sub("", 'Cannot find package %s for platform %s' % (pkg, platform)))
        elif found:
            uLogging.info("Will upgrade %s", package)
    return libs + single + non_single

def installPatches(sourcePatchesDir, con, host_id, upgrade_instructions):
    cur = con.cursor()

    cur.execute(
        "SELECT p.name, p.ctype, p.pkg_id, p.version FROM packages p JOIN components c ON (c.pkg_id = p.pkg_id) WHERE c.host_id = %s ORDER BY c.component_id", host_id)
    installed_packages = {}
    for row in cur.fetchall():
        installed_packages[(row[0], row[1])] = row[3]

    to_install = {}
    if upgrade_instructions is not None:
        for patch in upgrade_instructions.patches:
            name, ctype = patch
            if patch in installed_packages:
                uLogging.info('Going to install %s-%s patch because it is installed', name, ctype)
                to_install[patch] = upgrade_instructions.patches[patch]
    platform, rootpath = uPEM.getHostInfo(con, host_id)
    for nameAndCtype in to_install:
        name, ctype = nameAndCtype
        for patch in to_install[nameAndCtype]:
            if os.path.isabs(patch.fromPath):
                raise Exception("Source patch path '%s' is absolute, but it must be relative to %s directory" % (patch.fromPath, os.path.join(sourcePatchesDir, name + '-' + ctype, platform.os + '-' + platform.osver)))
            sourcePatchPath = os.path.join(sourcePatchesDir, name + '-' + ctype, platform.os + '-' + platform.osver, patch.fromPath)
            if not os.path.exists(sourcePatchPath):
                raise Exception("Path '%s' does not exist in source patch directory" % sourcePatchPath)
            if os.path.isabs(patch.toPath):
                targetPatchPath = patch.toPath
            else:
                targetPatchPath = os.path.join(rootpath, patch.toPath)

            r = uHCL.Request(host_id=host_id, user='root', group='root')
            r.transfer('1', sourcePatchPath, targetPatchPath)
            r.perform()
    return to_install


def updateMNModules():
    uAction.progress.do("Updating module packages on management node")
    api = openapi.OpenAPI()
    r = api.beginRequest()
    api.pem.packaging.updateMN()
    api.commit()
    openapi.waitRequestComplete(r, 'Updating packages on MN', True)
    uAction.progress.done()


def performFakeUpgrade(scs, rootpath):
    if not scs:
        return
    con = uSysDB.connect()
    cur = con.cursor()
    for sc, sc_rootpath in scs.values():
        if not sc_rootpath:
            sc_rootpath = rootpath
        new_location = '%s:%s' % (sc.package.name, os.path.join(sc_rootpath, sc.content.bin))

        cur.execute("UPDATE sc_instances SET location_id = %s WHERE sc_id IN (SELECT sc_id FROM service_classes WHERE name= %s)",
                    (new_location, sc.package.name))

    con.commit()

    uAction.progress.do("fake upgrading service controllers")

    cur.execute(
        "SELECT component_id, name FROM service_classes sc JOIN sc_instances si ON (si.sc_id = sc.sc_id) ORDER BY component_id")

    changed_scs_component_ids = [(x[0], x[1]) for x in cur.fetchall() if x[1] in scs]
    cur.close()
    con.commit()
    for sc in changed_scs_component_ids:
        uLogging.info('%s', sc)
        cid, name = sc
        cid = int(cid)
        uLogging.info("Upgrading %s(%s)", name, cid)
        uAction.retriable(uFakePackaging.upgradeSC)(cid)

    uAction.progress.done()


def repairPGSequences():
    if uSysDB.DBType != uSysDB.PgSQL:
        return

    con = uSysDB.connect()

    uLogging.debug("Repairing tables sequences")
    pattern = re.compile(r"^nextval\('(.*)'(:?::text|::regclass)?\)")

    try:
        cur = con.cursor()
        cur.execute(
            """
select
	n.nspname as schema_name,
	c.relname as table_name,
	a.attname as column_name,
	pg_get_expr(d.adbin, d.adrelid) as seq_name
from
	pg_class c
	join pg_attribute a on (c.oid=a.attrelid)
	join pg_attrdef d on (a.attrelid=d.adrelid and a.attnum=d.adnum)
	join pg_namespace n on (c.relnamespace=n.oid)
where
	n.nspname = 'public'
	and (not a.attisdropped)
	and d.adsrc like 'nextval%'
"""
        )
        seq_num = 0
        for seq in cur.fetchall():
            m = pattern.match(seq[3])
            seq_name = m.group(1)
            cur.execute("SELECT %s" % seq[3])
            cur.execute("""
SELECT SETVAL('%(seq_name)s', CASE WHEN lastval() > MAX(%(column_name)s) THEN lastval() ELSE MAX(%(column_name)s) END) FROM %(table_name)s
""" % { "seq_name": seq_name, "column_name": seq[2], "table_name": seq[1]} )
            seq_num += 1
        con.commit()
        uLogging.info("%d sequences were updated.", seq_num)
    finally:
        uSysDB.close(con)

def updateBrands():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT brand_id FROM brands")
    brands = [ row[0] for row in cur.fetchall() ]
    api = openapi.OpenAPI()

    for brand in brands:
        api.pem.refreshBrand(brand_id = brand)
