#!/usr/bin/python

import re
import time
import uLogging
import uSysDB

hotfix_pattern = re.compile(r'^\d\d\.\d\d_hotfix(\d\d)')


class PEMVersion:

    def __init__(self, version, hotfix=None):
        self.version = [int(x) for x in version.split('.')]

        if type(hotfix) in (int,):
            self.hotfix = hotfix
        elif hotfix is None:
            self.hotfix = None
        else:
            hfmatch = hotfix_pattern.match(hotfix)
            if hfmatch is None:
                self.hotfix = 0
            else:
                self.hotfix = int(hfmatch.group(1))

    def __cmp__(self, rhs):
        if type(rhs) in (str, unicode):
            to_cmp = PEMVersion(rhs)
        else:
            to_cmp = rhs

        if self.version == to_cmp.version:
            return cmp(self.hotfix, to_cmp.hotfix)
        else:
            return cmp(self.version, to_cmp.version)

    def __str__(self):
        rv = 'POA %s' % ('.'.join([str(x) for x in self.version]))
        if self.hotfix is not None:
            rv += ' hotfix %02d' % self.hotfix
        return rv

    def __repr__(self):
        return "<%s>" % self

    def version_same(self, rhs):
        if type(rhs) in (str, unicode):
            to_cmp = PEMVersion(rhs)
        else:
            to_cmp = rhs
        return to_cmp.version == self.version


def getPEMVersion():
    build_name, major_version, hotfixes = getCurrentVersion()
    if hotfixes:
        last_hf = hotfixes[-1]
    else:
        last_hf = None
    return PEMVersion(major_version, last_hf)

def getCurrentVersion():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT build, version FROM version_history WHERE install_date = (SELECT MAX(install_date) FROM version_history)")
    row = cur.fetchone()
    if not row:
        raise Exception("There is no product version registered, version_history is empty")
    build_name, major_version = row
    cur.execute("SELECT name FROM hotfixes WHERE build = %s ORDER BY name", build_name)
    hotfixes = cur.fetchall()
    hotfixes = map(lambda x: x[0], hotfixes)

    return build_name, major_version, hotfixes

def getCurrentUpdateAndHotfixes(build_name, hotfixes):
    updates = filter(lambda x: '_KB' not in x, hotfixes)
    if updates:
        current_update = updates[-1]
        update_for_hf_search = current_update
    else:   # this is RTM
        current_update = None
        update_for_hf_search = build_name
    hfs = filter(lambda x: '%s_KB' % update_for_hf_search in x, hotfixes)
    return current_update, hfs


def recordInstalledVersions(version_list):
    con = uSysDB.connect()
    cur = con.cursor()

    installed = time.time()
    versions_installed = []
    for row in version_list:
        name, version, built, kind = row
        versions_installed += [name]
        built = uSysDB.convertDTstring(built)
        idate = uSysDB.convertDTstring(time.asctime(time.localtime(installed)))

        uLogging.debug("recording installed build %s (version %s)", name, version)
        if kind == 'release':
            try:
                cur.execute(("INSERT INTO version_history(build, version, build_date, install_date) VALUES (%%s, %%s, %%s, %s() )" %
                             uSysDB.nowfun), (name, version, built))
            except Exception, e:
                try:
                    con.rollback()
                    cur.execute(
                        "UPDATE version_history SET version = %s, build_date = %s, install_date = %s WHERE build = %s", (version, built, idate, name))
                    uLogging.warn("%s had already been installed", name)
                except Exception, e:
                    uLogging.err("%s %s", e, e.args)
        else:
            try:
                cur.execute(
                    "INSERT INTO hotfixes (name, build, build_date, install_date) SELECT %s, build, %s, %s FROM version_history WHERE install_date = (SELECT MAX(install_date) FROM version_history)", (name, built, idate))
            except Exception, e:
                try:
                    con.rollback()
                    cur.execute(
                        "UPDATE hotfixes SET build_date = %s, install_date = %s WHERE name = %s", (built, idate, name))
                    uLogging.warn("%s had already been installed", name)
                except Exception, e:
                    uLogging.err("%s %s", e, e.args)

        installed += 1

    con.commit()
    return versions_installed


def getSCVersion(name):
    con = uSysDB.connect()
    cur = con.cursor()

    cur.execute("SELECT version FROM service_classes WHERE name = %s", name)

    row = cur.fetchone()
    if not row:
        raise Exception("Service controller '%s' is not installed" % name)

    return PEMVersion(row[0])


def parseVersionDetail(version_str):
    """
    Parse hotfix or update name
        :param version_str: e.g. "oa-7.1-custom999_update01_KB12345-666"

    Returns:
        :return version: e.g. "oa-7.1-custom999"
        :return update: e.g. "01"
        :return hotfix: e.g. "12345"
        :return major: e.g. "7"
        :return minor: e.g. "1"
    """

    def none_or_match(regexp, string_for_search, group_num):
        m = re.match(regexp, string_for_search)
        if m:
            return m.group(group_num)

    version = none_or_match("^oa-\d+\.\d+-[a-zA-Z_0-9]+(?=(_update\d{2}|(?<!update\d{2})_KB\d{4,})|$)", version_str, 0)
    update = none_or_match(".*_update(\d{2})(_|-)", version_str, 1)
    hotfix = none_or_match(".*_KB(\d{4,})-", version_str, 1)
    major = none_or_match("^oa-(\d).(\d)-[a-zA-Z_0-9]", version_str, 1)
    minor = none_or_match("^oa-(\d).(\d)-[a-zA-Z_0-9]", version_str, 2)

    return version, update, hotfix, major, minor

def parseVersion(version_str):
    version, update, hotfix, _, _ = parseVersionDetail(version_str)
    return version, update, hotfix

def parseProductVersion(version_str):
    """
    convert build name (like "poa-5.2-23", "poa-5.2-435_update01") into
    descriptive product name and version like ("POA", "5.2"), ("POA", "5.0 update01")
    :param version_str: string containing version
    :return: tuple (product, version) of two strings
    """
    product = version = None
    pattern = re.compile(r"^(oa|poa|ppa)-(\d+\.\d+)-\w+_?(update\d{2})?")
    m = pattern.match(version_str)
    if m:
        product = m.group(1).upper()
        version = m.group(2)
        if m.group(3):
            version += " " + m.group(3)

    return product, version

def __cmpJustVer(v1, v2):
    maj1, min1 = v1
    maj2, min2 = v2
    if maj1 and min1:
        if not (maj2 and min2):
            return 1
        if maj1 < maj2:
            return -1
        if maj1 > maj2:
            return 1

        if min1 < min2:
            return -1
        if min1 > min2:
            return 1
        return 0
    elif maj2 and min2:
        return -1

    return 0


def compareVersions(v1, v2):
    v1v, v1u, _, major1, minor1 = parseVersionDetail(v1)
    v2v, v2u, _, major2, minor2 = parseVersionDetail(v2)

    if not v1v or not v2v:
        return 0
    rv = __cmpJustVer((major1, minor1), (major2, minor2))
    if rv:
        return rv

    if not v1u:
        if not v2u:
            return 0
        else:
            return -1
    elif not v2u:
        return 1

    return cmp(v1u, v2u)
