import sys
import os
import re
from openapi import OpenAPI
import uSysDB
import uLogging
try:
    import json
except ImportError:
    import simplejson as json

from multiprocessing.dummy import Pool as ThreadPool
from uConst import Const

class ApplicationInstance:

    def __init__(self, application_instance_id=None, resource_id=None):
        self.id = application_instance_id
        self.resource_id = resource_id

    def unprovide(self):
        api = OpenAPI()
        api.pem.APS.unprovideApplicationInstance(application_instance_id=self.id)


class ApplicationInstanceExt(ApplicationInstance):

    def __init__(self, application_instance_id, application_id, url, rt_id, status, package_version, application_resource_id):
        ApplicationInstance.__init__(self, application_instance_id, application_resource_id)
        self.application_id = application_id
        self.url = url
        self.rt_id = rt_id
        self.status = status
        self.package_version = package_version

    def get_version(self):
        return self.package_version


class Application:

    def __init__(self, application_id=None, version=None):
        self.id = application_id
        self.version = version
        pass

    def createInstance(self, endpoint, address='localhost', open_api_req='App_Instance'):
        if not self.id:
            raise Exception('Application is not imported')

        api = OpenAPI()
        api.beginRequest(request_id_cand=open_api_req, prefix_gen=None)
        api.pem.APS.provideApplicationInstance(
            subscription_id=0, app_id=self.id, package_version=self.version, url_path=endpoint, rt_id=0)
        api.commit()

    def upgradeInstance(self, application_instance_id, open_api_req='App_Instance_Upgrade'):
        if not self.id:
            raise Exception('Application is not imported')
        inst = self.getInstance(application_instance_id)
        ver = inst.get_version()
        if ver == self.version:
            return

        api = OpenAPI()
        api.beginRequest(request_id_cand=open_api_req, prefix_gen=None)
        api.pem.APS.upgradeApplicationInstance(
            application_instance_id=application_instance_id, package_version=self.version)
        api.commit()

        pass

    def remove(self, open_api_req='App'):
        api = OpenAPI()
        api.beginRequest(request_id_cand=open_api_req, prefix_gen=None)
        api.pem.APS.removeApplication(application_id=self.id)
        api.commit()
        self.id = None
        self.version = None
        pass

    def getInstances(self):
        if not self.id:
            raise Exception('Application is not imported')

        api = OpenAPI()
        res = api.pem.APS.getApplicationInstances(app_id=self.id)

        rv = []
        for app in res:
            rv.append(ApplicationInstance(app['application_instance_id'], app['application_resource_id']))

        return rv

    def getInstance(self, application_instance_id):
        if not self.id:
            raise Exception('Application is not imported')

        api = OpenAPI()
        res = api.pem.APS.getApplicationInstance(application_instance_id=application_instance_id)
        return ApplicationInstanceExt(res['application_instance_id'], res['application_id'], res['url'], res['rt_id'], res['status'], res['package_version'], res['application_resource_id'])


class ApplicationExt(Application):

    def __init__(self, application_id, version, name=None, owner_id=None, app_url_uid=None, package_source_url=None, pkg_uuid=None, aps_package_id=None):
        Application.__init__(self, application_id, version)
        self.name = name
        self.owner_id = owner_id
        self.app_url_uid = app_url_uid
        self.package_source_url = package_source_url
        self.pkg_uuid = pkg_uuid
        self.aps_package_id = aps_package_id


def import_app(URL):
    from urlparse import urlparse
    app_URI = URL
    if not urlparse(app_URI)[0]:
        app_URI = __getApplicationPackage(app_URI)

    api = OpenAPI()
    res = api.pem.APS.importPackage(package_url=app_URI)
    return Application(res['application_id'], res['package_version'])


def get(uri_application_id):
    api = OpenAPI()
    applications = api.pem.APS.getApplications(aps_application_id=uri_application_id)
    if not applications:
        raise Exception('Application with URI id "%s" is not found' % uri_application_id)
    return Application(applications[0]['application_id'], None)


def __formatLocalApp(path):
    from urllib import pathname2url
    return 'file:///' + pathname2url(path).lstrip('/')


def __getApplicationPackage(name):
    if os.path.isfile(name):
        return __formatLocalApp(name)

    from poaupdater import uPEM
    import re

    root = '%s/install/APS' % uPEM.getMNInfo()[1]

    global pkgFile
    pkgFile = '%s/%s' % (root, name)
    if os.path.isfile(pkgFile):
        return __formatLocalApp(pkgFile)

    aps_pattern = re.compile(r'%s[.\-]([0-9.\-]+)\.app\.zip$' % name)

    def findPackage(arg, dirname, names):
        max_ver = []
        for fn in names:
            m = re.match(arg, fn)
            if m:
                current_ver = [int(x) for x in re.split("[.\-]", m.group(1)) if x]
                if max_ver > current_ver:
                    continue
                max_ver = current_ver
                global pkgFile
                pkgFile = '%s/%s' % (root, fn)

    os.path.walk(root, findPackage, aps_pattern)

    if pkgFile == '%s/%s' % (root, name):
        raise Exception('Package for "%s" is not found' % name)
    return __formatLocalApp(pkgFile)


def getApplicationEx(app_id):

    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        """SELECT aa.app_id, ap.app_ver, aa.name, aa.owner_id, aa.app_uid, ap.url, ap.uuid, ap.pkg_id FROM aps_applications aa JOIN aps_packages ap ON ap.app_id = aa.app_id WHERE aa.app_id = %s""" % app_id)

    rows = cur.fetchall()

    if rows:
        application_id, version, name, owner_id, app_url_uid, package_source_url, pkg_uuid, aps_package_id, = rows[0]
        return ApplicationExt(application_id=application_id, version=version, name=name, owner_id=owner_id, app_url_uid=app_url_uid, package_source_url=package_source_url, pkg_uuid=pkg_uuid, aps_package_id=aps_package_id)
    else:
        raise Exception('Application with app_id %s can not be found in DB' % app_id)


def is_aps_db_installed(con):
    return uSysDB.table_exist(con, "aps_db_version")


def get_db_type():
    db_type = None
    if sys.platform == 'win32':
        db_type = "mssql"
    else:
        db_type = "pgsql"
    return db_type


def get_db_version(con):
    cur = con.cursor()
    cur.execute("select vmajor, vminor from aps_db_version")
    db_ver = cur.fetchall()[0]
    db_vmajor = int(db_ver[0])
    db_vminor = int(db_ver[1])
    return (db_vmajor, db_vminor)


def set_db_version(con, vmajor, vminor):
    cur = con.cursor()
    cur.execute(
        "update aps_db_version set vmajor=%d, vminor=%d" % (vmajor, vminor))


def list_db_scripts(dir, pattern):
    sc_list = []
    for sc in sorted(os.listdir(dir)):
        if pattern.search(sc):
            sc_list.append(sc)
    return sc_list


def get_update_scripts_pattern():
    regex = "dbschema-%s\.(\d{2})-(\d{3})\.sql" % get_db_type()
    pattern = re.compile(regex)
    return pattern


def upgrade_aps_db(scriptsDir, con=None):
    if con is None:
        con = uSysDB.connect()
    # To be sure
    if not is_aps_db_installed(con):
        install_aps_db(scriptsDir, con)

    db_ver = get_db_version(con)
    uLogging.debug("APS DB v%d.%d found." % (db_ver[0], db_ver[1]))

    uLogging.info("Looking for application upgrades in '%s'" % scriptsDir)

    cursor = con.cursor()
    upgrade_found = None
    sc_pattern = get_update_scripts_pattern()
    sc_list = list_db_scripts(scriptsDir, sc_pattern)
    for path in sc_list:
        sc_matcher = sc_pattern.match(path)
        sc_ver = (int(sc_matcher.group(1)), int(sc_matcher.group(2)))
        if sc_ver[0] > db_ver[0] or (sc_ver[0] == db_ver[0] and sc_ver[1] > db_ver[1]):
            path = os.path.join(scriptsDir, path)
            execute_db_script(path, cursor)
            set_db_version(con, sc_ver[0], sc_ver[1])
            con.commit()
            uLogging.info("'%s' applied." % path)
            upgrade_found = True

    db_ver = get_db_version(con)
    uSysDB.close(con)

    if upgrade_found:
        uLogging.info("APS DB upgraded to v%d.%d." % (db_ver[0], db_ver[1]))
    else:
        uLogging.info("No new upgrades for APS DB found.")


def find_sql_script_meta(script):
    rv = None
    # regexp for searching internal content of multiline comments like /**blah*/)
    # group is named as this is useful to get found text between /** and */ directly
    xmeta = re.compile(r"(?:/\*\*(?P<META>[\S\s]*?)\*/)", re.I|re.M);
    mtch = xmeta.search(script)
    while mtch:
        jmeta = mtch.group("META")
        if jmeta:
            metaDict = None
            try:
                metaDict = json.loads(jmeta)
            except:
                pass
            if metaDict:
                rv = metaDict.get("execution-meta-data", None)
                if rv:
                    break
        script = script[mtch.span()[1]:]
        mtch = xmeta.search(script)
    return rv


def split_sql_script(script):
    # ORed two regexp groups for searching comments (sinle line comment like --blah) or (multiline comments like /*blah*/)
    xcmmnt = re.compile(r"(?:--[^\r\n]*)|(?:/\*[\S\s]*?\*/)", re.I|re.M);

    # ORed regexp groups for searching one of the following
    # - sinle line comment like --blah
    # - multiline comments like /*blah*/
    # - single quoted multiline strings like 'blah'
    # - double quoted strings like "blah"
    # - statements separator aka ; (group is named as this is a key item in this regexp)
    xgosep = re.compile(r"(?:--[^\r\n]*)|(?:/\*[\S\s]*?\*/)|(?:'([^\\']|\\.)*')|(?:\"[^\"\\r\n]*(?:\\.[^\"\\\r\n]*)*\")|(?:(?P<GO>;)(?:--[^\r\n]*)?)", re.I|re.M);

    rv = []
    portn = ""
    mtch = xgosep.search(script)
    while mtch:
        rng = mtch.span()[1]
        portn += script[:rng]
        script = script[rng:]
        if mtch.group('GO'):
            if len(re.sub(xcmmnt, "", portn).strip(" \r\n\t;")) > 0:
                rv.append(portn)
            portn = ""
        mtch = xgosep.search(script)

    if len(re.sub(xcmmnt, "", script).strip(" \r\n\t;")) > 0:
        rv.append(script)
    return rv


def onDBScriptPart(sqlPart):
    connect = uSysDB.PerThreadConnections.getConnection()
    if isinstance(connect, uSysDB.Connection):
        try:
            cursr = connect.cursor()
            cursr.execute(sqlPart)
            connect.commit()
        except Exception, e:
            uSysDB.PerThreadConnections.onError(e)


def execute_db_script(path, cursor=None):
    assert os.path.exists(path)

    isf = open(path, 'r')
    script = isf.read()
    isf.close()

    runMode = "entirely" #statement-by-statement or statements-in-parallel
    sqlmeta = find_sql_script_meta(script)
    if sqlmeta:
        runMode = sqlmeta.get("run-mode", runMode)

    if runMode and runMode in ("statement-by-statement", "statements-in-parallel"):
        parts = split_sql_script(script)
        if runMode == "statements-in-parallel":
            try: 
                pool = ThreadPool()
                pool.map(onDBScriptPart, parts)
                pool.close()
                pool.join()
            finally:
                uSysDB.PerThreadConnections.closeAll()
        else:
            if cursor is None:
                con = uSysDB.connect()
                cursor = con.cursor()
            for q in parts:
                cursor.execute(q)
                con.commit()
    else:
        if cursor is None:
            con = uSysDB.connect()
            cursor = con.cursor()
        cursor.execute(script)


def install_aps_db(scriptsDir, con=None):
    if con is None:
        con = uSysDB.connect()

    initial = os.path.join(scriptsDir, "dbschema-%s.sql" % get_db_type())
    uLogging.info("Installing APS database from %s" % initial)

    execute_db_script(cursor=con.cursor(), path=initial)
    con.commit()

    db_ver = get_db_version(con)
    uSysDB.close(con)

    uLogging.info("New APS DB v%d.%d installed." % (db_ver[0], db_ver[1]))


def performMSSQLUpgrade(con):
    dropPageLocksTSQLScript = """
    DECLARE @aps_tables TABLE(tbl_name [nvarchar](256) PRIMARY KEY NOT NULL)
    INSERT INTO @aps_tables (tbl_name)
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_TYPE='BASE TABLE') AND ((TABLE_NAME LIKE 'aps_%s') OR (TABLE_NAME LIKE 'saas_%'))
    DECLARE @tbl AS [nvarchar](256)
    SELECT TOP 1 @tbl=tbl_name FROM @aps_tables
    WHILE @tbl IS NOT NULL
    BEGIN
        EXEC sp_indexoption @tbl, 'AllowRowLocks', TRUE
        EXEC sp_indexoption @tbl, 'AllowPageLocks', FALSE
        DELETE FROM @aps_tables WHERE tbl_name = @tbl
        SET @tbl = NULL
        SELECT TOP 1 @tbl=tbl_name FROM @aps_tables
    END
    """
    cur = con.cursor()
    cur.execute(dropPageLocksTSQLScript)


def performAPSTypesUpgrade(con, binfo):
    import tarfile
    import tempfile
    import shutil
    from poaupdater import uPEM, uBuild

    def importAPSTypePackages(aps_core_types_pkg, poa_core_services_pkg):
        assert os.path.exists(aps_core_types_pkg)
        assert os.path.exists(poa_core_services_pkg)

        caFilename = uPEM.getSCProperty(con, 'SaaS', 'saas.aps.restful.server.CA_filename')
        serverPort = uPEM.getSCProperty(con, 'SaaS', 'saas.aps.resources.end.point.server.port')
        serverPath = uPEM.getSCProperty(con, 'SaaS', 'saas.aps.resources.end.point.server.path')
        certFilename = uPEM.getSCProperty(con, 'SaaS', 'saas.aps.resources.end.point.server.certificate_filename')

        for x in (False, True):
            uLogging.info("importing POA-Core-Services package.")
            try:
                uPEM.execCtl('saas_ctl', ('install', serverPort, serverPath, poa_core_services_pkg, caFilename, certFilename))
            except Exception, e:
                if x:
                    raise e

            if not x:
                uLogging.info("importing APSv2-Core-Types package.")
                uPEM.execCtl('saas_ctl', ('import', aps_core_types_pkg))

    aps_core_types_pkg = 'APSv2-Core-Types.app.zip'
    poa_core_services_pkg = 'POA-Core-Services.app.zip'
    saas_ctl = 'saas_ctl'
    libaps_so = 'libaps.so'
    SaaS_so = 'SaaS.so'

    platform, root = uPEM.getMNInfo()
    bin = os.path.join(root, 'bin')
    lib = os.path.join(root, 'lib')
    libexec = os.path.join(root, 'libexec')

    tempdir = tempfile.mkdtemp()
    try:
        for build in binfo.builds:
            uLogging.info("Looking for suitable binaries to import package.")
            suitable_platform_line = uPEM.getPlatformLine(con, platform)
            package_content = None
            for suitable_platform in suitable_platform_line:
                package = uBuild.Package('SaaS', 'sc', suitable_platform)
                if build.contents.has_key(package):
                    package_content = build.contents[package]
                    uLogging.info("Found suitable built binaries for platform %s" % suitable_platform)
                    break
            if package_content:
                uLogging.info("Unpack SaaS binary utilities...")
                tarball = build.find_valid_tarball(package_content)
                tar = tarfile.open(tarball)
                # extract libexec/SaaS.so*, bin/saas_ctl and lib/libaps.so
                for tarinfo in tar.getmembers():
                    if os.path.basename(tarinfo.name).startswith(SaaS_so):
                        tarinfo.name = os.path.basename(tarinfo.name)
                        tar.extract(tarinfo, path=libexec)
                    if os.path.basename(tarinfo.name) == libaps_so:
                        tarinfo.name = os.path.basename(tarinfo.name)
                        tar.extract(tarinfo, path=lib)
                    if os.path.basename(tarinfo.name) == saas_ctl:
                        tarinfo.name = os.path.basename(tarinfo.name)
                        tar.extract(tarinfo, path=bin)
                uLogging.info("SaaS binaries unpacked.")

            for pkg in build.contents.values():
                if pkg.package.name == 'SaaS':
                    tarball = build.find_valid_tarball(pkg)
                    tar = tarfile.open(tarball)
                    # extract types from packages
                    for tarinfo in tar.getmembers():
                        if tarinfo.name.endswith(aps_core_types_pkg) or tarinfo.name.endswith(poa_core_services_pkg):
                            tarinfo.name = os.path.basename(tarinfo.name)
                            tar.extract(tarinfo, path=tempdir)

                    importAPSTypePackages(os.path.join(tempdir, aps_core_types_pkg),
                                          os.path.join(tempdir, poa_core_services_pkg))
                    break

    finally:
        shutil.rmtree(tempdir)


def performAPS20ConsolidatedUpgrade(binfo):
    perform_aps2_db_upgrade(binfo)
    perform_aps2_sys_pkgs_upgrade(binfo)

def perform_aps2_db_upgrade(binfo):
    from poaupdater import uAction

    scripts_dirs = binfo.upgrade_instructions.aps
    if not scripts_dirs:
        uLogging.info(
            "There is no APS Database upgrade scripts. Skip upgrading.")
        return

    uAction.progress.do("updating APS database")
    con = uSysDB.connect()
    for scripts_dir in scripts_dirs:
        upgrade_aps_db(scripts_dir, con)

    if Const.isWindows():
        uAction.progress.do("dropping APS database page locking")
        performMSSQLUpgrade(con)
        uAction.progress.done()

    uAction.progress.done()
    uSysDB.close(con)

def perform_aps2_sys_pkgs_upgrade(binfo):
    from poaupdater import uPEM, uAction

    if not uPEM.is_sc_installed("SaaS"):
        uLogging.info("No 'SaaS' SC installed found. Skip bundled APS Type's packages importing.")
        return

    con = uSysDB.connect()
    uAction.progress.do("importing APS Type's packages")
    performAPSTypesUpgrade(con, binfo)
    uAction.progress.done()
    uSysDB.close(con)
