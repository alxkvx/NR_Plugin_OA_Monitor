#!/usr/bin/python

__rcs_id__ = """$Id$"""
__pychecker__ = "no-import unusednames=__rcs_id__,dummy"

import cStringIO
import os
from xml.dom import minidom as dom

import uSysDB
import uModularOpenAPI
import uManifestParser
import uLogging
import uPEM
import uUtil
import uPackaging
import openapi

def upgradeSC(component_id):
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("""SELECT sc.sc_id, p.name, p.version, p.pkg_id, sc.sc_group_id, sc.name
		FROM service_classes sc
		JOIN sc_instances si ON (sc.sc_id = si.sc_id)
		JOIN components c ON (c.component_id = si.component_id)
		JOIN packages p ON (p.pkg_id = c.pkg_id)
		WHERE si.component_id = %s""", component_id)
    row = cur.fetchone()
    if not row:
        raise Exception("Component id %s does not exist, or not a service controller" % component_id)
    pkg_name, pkg_version, sc_id, old_pkg_id, sc_group_id, scname = row[1], row[2], row[0], row[3], row[4], row[5]
    uLogging.debug("Upgrading %s-%s (sc_id = %d, pkg_id = %d)", pkg_name, pkg_version, sc_id, old_pkg_id)

    mn_platform, plesk_root = uPEM.getMNInfo()
    platforms = uPEM.getPlatformLine(con, mn_platform)
    for platform in platforms:
        cur.execute("""SELECT MAX(p.pkg_id)
            FROM packages p
            JOIN packages p2 ON (p.name = p2.name AND p.ctype = p2.ctype)
            WHERE p2.pkg_id = %s
            AND p.pkg_id >= p2.pkg_id
            AND p.platform_id = %s""", old_pkg_id, platform.platform_id)
        row = cur.fetchone()
        if row[0]:
            uLogging.debug("Higher version package found: %s (platform = %s, pkg_id = %d)", pkg_name, platform, row[0])
            break

    if not row[0]:
        uLogging.warn("No higher version package found")
        return
    new_pkg_id = row[0]
    # TODO: only one service_instance should be auto-updated, rest ones should refer to old components

    cur.execute("UPDATE components SET pkg_id = %s WHERE component_id = %s", (new_pkg_id, component_id))
    cur.execute(
        "UPDATE service_classes SET version = p.version FROM packages p WHERE sc_id = %s AND p.pkg_id = %s", (sc_id, new_pkg_id))

    # update component properties
    cur.execute("""
	   SELECT p.prop_id, p.name, p2.prop_id AS new_prop_id
	   FROM component_properties cp
	   JOIN properties p ON (cp.prop_id = p.prop_id)
	   LEFT JOIN properties p2 ON (p.name = p2.name)
	   WHERE p.pkg_id = %s
	   AND p2.pkg_id = %s
	   AND cp.component_id = %s """, (old_pkg_id, new_pkg_id, component_id))

    rows = cur.fetchall()

    for row in rows:
        old_prop_id, prop_name, new_prop_id = row[0], row[1], row[2]
        if new_prop_id is None:
            uLogging.debug("Deleting property %s (old prop_id %s)", prop_name, old_prop_id)
            cur.execute("""DELETE FROM component_properties
	                    WHERE component_id = %s AND prop_id = %s """, (component_id, old_prop_id))
        else:
            uLogging.debug('Copying property %s (old prop_id %s, new prop_id %s)', prop_name, old_prop_id, new_prop_id)
            cur.execute("""UPDATE component_properties
			    SET prop_id = %s
	                    WHERE component_id = %s
	                    AND prop_id = %s """, (new_prop_id, component_id, old_prop_id))

    # register new properties of updated SC except (they should be set manually in upgrade procedure):
    # 1. mandatory properties with not specified default
    # 2. properties which should be got by depend (TODO, probably updater should resolve such depends)
    cur.execute("""
	   SELECT
	   p.default_value, p.name
	   FROM properties p
	   WHERE p.pkg_id = %s
	   AND NOT EXISTS (SELECT 1 FROM component_properties cp WHERE cp.component_id = %s AND cp.prop_id = p.prop_id)
	   AND (p.default_value IS NOT NULL or p.mandatory = 'n')
	   AND NOT EXISTS (SELECT 1 FROM dep_get_properties dp 
	   JOIN package_dependencies pd ON pd.dep_id = dp.dep_id WHERE pd.pkg_id = p.pkg_id AND dp.name = p.name)
	   """, (new_pkg_id, component_id))
    rows = cur.fetchall()
    for row in rows:
        prop_val, prop_name = row[0], row[1]
        uLogging.debug('Register SC %s missing property %s (component_id %s, default_value %s)',
                       scname, prop_name, component_id, prop_val)
        uPEM.setSCProperty(con, scname, prop_name, prop_val)

    # update OpenAPI methods and observer interfaces
    cur.close()
    cur = con.cursor()
    cur.execute("SELECT data FROM package_body WHERE pkg_id = %s", new_pkg_id)
    row = cur.fetchone()
    data = row[0]
    data = str(data)
    manifest = uManifestParser.unsign_package(cStringIO.StringIO(data))

    xml = dom.parseString(manifest)
    observer_interfaces = [node.getAttribute("name") for node in xml.getElementsByTagName(
        "INTERFACE") if node.getAttribute("observer") == "yes"]
    uLogging.debug("Attempting to register observer interfaces: %s", ', '.join(
        ["%s" % service_type for service_type in observer_interfaces]))
    iparams = [sc_id]
    for service_type in observer_interfaces:
        iparams += [service_type]
        cur.execute("""
		INSERT INTO observers(sc_id, interface_id)
		SELECT %s, interface_id FROM interfaces i WHERE
		service_type = %s AND NOT EXISTS
		(SELECT 1 FROM observers o WHERE sc_id = %s AND i.interface_id = o.interface_id)""", (sc_id, service_type, sc_id))

    if not observer_interfaces:
        cur.execute("DELETE FROM observers WHERE sc_id = %s", sc_id)
    else:
        cur.execute(("DELETE FROM observers WHERE sc_id = %%s AND interface_id NOT IN (SELECT interface_id FROM interfaces WHERE (%s))" %
                     ' OR '.join(['(service_type = %s)'] * len(observer_interfaces))), iparams)

    cur.execute(
        "DELETE FROM openapi_methods WHERE extension_id IN "
        "(SELECT extension_id FROM openapi_extensions WHERE sc_id = %s)", sc_id)
    cur.execute(
        "DELETE FROM openapi_extensions WHERE sc_id = %s", sc_id)

    new_sc_group_id = None
    if pkg_version >= "2.9":  # upgrade SC group id
        sc_group_ids = [x.getAttribute("value") for x in xml.getElementsByTagName(
            "ATTRIBUTE") if x.getAttribute("name") == "SC_GROUP_ID"]
        if not sc_group_ids:
            uLogging.debug("Cannot find SC Group ID for %s-%s", pkg_name, pkg_version)
        else:
            try:
                new_sc_group_id = int(sc_group_ids[0])
                uLogging.debug("New SC group id = %s, old = %s", new_sc_group_id, sc_group_id)
            except Exception:
                uLogging.err("%s: invalid sc_group_id", sc_group_ids[0])
    if new_sc_group_id is not None and sc_group_id != new_sc_group_id:
        cur.execute("UPDATE service_classes SET sc_group_id = %s WHERE sc_id = %s", new_sc_group_id, sc_id)

    fixDependencies(con, component_id, new_pkg_id, pkg_name)

    uModularOpenAPI.update_extensions_from_xml(xml, con)
    con.commit()
    uSysDB.close(con)


class ComponentProvide:

    def __init__(self, con, component_id):
        cur = con.cursor()
        cur.execute("SELECT pkg_id, host_id FROM components WHERE component_id = %s", component_id)
        row = cur.fetchone()
        self.pkg_id = row[0]
        self.component_id = component_id
        self.host_id = row[1]
        cur.execute("SELECT name, version, ctype FROM packages WHERE pkg_id = %s", self.pkg_id)
        row = cur.fetchone()
        self.pkg_name = row[0]
        self.pkg_ctype = row[2]
        self.pkg_version = row[1]

        cur.execute(
            "SELECT i.interface_id, i.service_type FROM interfaces i JOIN package_interfaces pi ON (pi.interface_id = i.interface_id) WHERE pi.pkg_id = %s", self.pkg_id)
        self.interfaces = [(row[0], row[1]) for row in cur.fetchall()]

    def satisfies(self, dep):
        if dep.package:
            name, ctype, version = dep.package
            ok = self.pkg_name == name and self.pkg_ctype == ctype
            if ok:
                uLogging.debug("%s-%s(%s) satisfies package dependency %s", self.pkg_ctype,
                               self.pkg_name, self.component_id, dep.dep_id)
            return ok
        elif dep.interface:
            if_id, service_type = dep.interface
            pi = [iface for iface in self.interfaces if iface[0] == if_id]
            if pi:
                uLogging.debug("%s-%s(%s) satisfies interface dependency %si on %s", self.pkg_ctype,
                               self.pkg_name, self.component_id, dep.dep_id, service_type)
                return True
        return False


class PkgDepend:

    def __init__(self, con, dep_id, dep_type):
        self.same_host = dep_type == 'H'
        self.dep_id = dep_id
        cur = con.cursor()
        cur.execute("SELECT name, ctype, version FROM dep_packages WHERE dep_id = %s", self.dep_id)
        row = cur.fetchone()
        if row:
            self.package = (row[0], row[1], row[2])
        else:
            self.package = None

        cur.execute(
            "SELECT i.interface_id, i.service_type FROM dep_interfaces di JOIN interfaces i ON (i.interface_id = di.interface_id) WHERE di.dep_id = %s", self.dep_id)
        row = cur.fetchone()
        if row:
            self.interface = (row[0], row[1])
        else:
            self.interface = None
        if self.package is None and self.interface is None:
            uLogging.err("%d: could not determine dependency kind", dep_id)

    def __str__(self):
        if self.package:
            return "pkg %s-%s" % (self.package[0], self.package[1])
        elif self.interface:
            return "interface %s" % self.interface[1]
        return "incorrect depend %d" % self.dep_id


def fixDependencies(con, dep_id, pkg_id, name):
    cur = con.cursor()
    cur.execute("SELECT component_id FROM component_dependencies WHERE dep_id = %s", dep_id)
    depends_on = [row[0] for row in cur.fetchall()]
    depends_on = [ComponentProvide(con, c) for c in depends_on]
    cur.execute(
        "SELECT dep_id, dep_type FROM package_dependencies WHERE pkg_id = %s AND dep_type IN ('H', 'S')", pkg_id)
    pkg_depends_on = [(row[0], row[1]) for row in cur.fetchall()]
    pkg_depends_on = [PkgDepend(con, *d) for d in pkg_depends_on]

    redundant_depends = depends_on[:]
    not_satisfied_depends = pkg_depends_on[:]
    for c in depends_on:
        for dep in pkg_depends_on:
            if c.satisfies(dep):
                redundant_depends.remove(c)
                not_satisfied_depends.remove(dep)
    for c in redundant_depends:
        uLogging.debug("%s no longer depends on %s-%s(%s)", name, c.pkg_ctype, c.pkg_name, c.component_id)
        cur.execute(
            "DELETE FROM component_dependencies WHERE dep_id = %s AND component_id = %s", dep_id, c.component_id)

    for dep in not_satisfied_depends:
        uLogging.debug("New depend for package %s %s", name, dep)


def getNodeByTagName(doc, tagname, err_package=None):
    rv = doc.getElementsByTagName(tagname)

    if not rv:
        if not err_package:
            return None
        else:
            raise Exception("Malformed manifest %s: no %s node" % (err_package, tagname))
    return rv[0]


def __getInterfaceId(con, node):
    service_type = node.getAttribute("name")
    cur = con.cursor()
    cur.execute("SELECT interface_id FROM interfaces WHERE service_type = %s", service_type)
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("INSERT INTO interfaces(service_type) VALUES (%s)", service_type)

    return uSysDB.get_last_inserted_value(con, "interfaces")

def installMetaPackage(host_id, pkg_list=None, pkg_id_list=None):
    uLogging.debug('installMetaPackage to host %s: %s %s' % (host_id, pkg_list, pkg_id_list))
    api = openapi.OpenAPI()
    pkgs_to_install = [];
    if pkg_list:
        for pname, ptype in pkg_list:
            try:
                pkg_id, ver = uPackaging.findSuitablePackage(host_id, pname, ptype)
                pkgs_to_install.append(pkg_id)
            except uPackaging.PackageNotFound, e:
                uLogging.debug("Package %s-%s was not found for host %s: %s", ptype, pname, host_id, e)
    elif pkg_id_list:
        pkgs_to_install = pkg_id_list
    else:
        return  # nothing to install
    uSysDB.connect().rollback()
    api.pem.packaging.installPackageSync(host_id = host_id, package_list = pkgs_to_install, properties=[])

def registerComponent(con, host_id, pkg_id, rootpath):
    component_id = con.insertRecordWithId("components", host_id=host_id, pkg_id=pkg_id, rootpath=rootpath)
    cur = con.cursor()

    # set default value for properties, which could not be taken by depend
    cur.execute(
        "SELECT p.prop_id, p.default_value FROM properties p WHERE p.pkg_id = %s AND NOT EXISTS (SELECT 1 FROM dep_get_properties dgp JOIN package_dependencies pd ON (pd.dep_id = dgp.dep_id AND p.name = dgp.name AND pd.pkg_id = p.pkg_id))", pkg_id)
    for row in cur.fetchall():
        value_id = con.insertRecordWithId("property_values", value=row[1], component_id=component_id)
        con.insertRecord("component_properties", prop_id=row[0], value_id=value_id, component_id=component_id)

    # set values by dependencies, XXX: too complex, copy-n-pasted from plesk_data.h
    cur.execute("""INSERT INTO component_properties(component_id, prop_id, value_id)
                SELECT %s, data.to_prop, cp.value_id
                FROM 
                (SELECT DISTINCT
                CASE WHEN res.one_id IS NULL THEN res.two_id
                ELSE res.one_id END as from_pkg,
                res.prop_id as to_prop,
                res.depname as from_prop
                FROM (SELECT 
                        p.pkg_id as one_id, 
                        pi.pkg_id as two_id,
                        pr.prop_id as prop_id,
                        dgp.depname as depname 
                        FROM dep_get_properties dgp 
                        JOIN package_dependencies pd ON (dgp.dep_id = pd.dep_id)
                        JOIN components c ON (c.pkg_id = pd.pkg_id)
                        JOIN properties pr ON
                                (pr.name = dgp.name AND pd.pkg_id = pr.pkg_id)
                        JOIN packages pp ON (pp.pkg_id = pd.pkg_id)
                        LEFT JOIN dep_packages dp ON (dp.dep_id = pd.dep_id)
                        LEFT JOIN packages p ON (p.name = dp.name 
                          AND (dp.version = '' OR p.version = dp.version) AND p.ctype = dp.ctype)
                        LEFT JOIN dep_interfaces di ON (di.dep_id = pd.dep_id)
                        LEFT JOIN package_interfaces pi ON
                                (pi.interface_id = di.interface_id)
                        WHERE c.component_id = %s) as res
                        ) as data 
                JOIN components c ON (c.pkg_id = data.from_pkg)
                JOIN component_properties cp ON 
                        (cp.component_id = c.component_id)
                JOIN properties p ON
                        (p.prop_id = cp.prop_id AND p.name = data.from_prop);
		""", component_id, component_id)
    return component_id


def location_id(pkg, rootpath):
    return "%s:%s" % (pkg.package.name, os.path.join(rootpath, pkg.content.bin))


def registerSC2(con, name, version, user_id, sc_group_id, loader_id, component_id, location_id):
    sc_id = con.insertRecordWithId(
        "service_classes", name=name, version=version, user_id=user_id, sc_group_id=sc_group_id, required_state='a')
    sc_instance_id = con.insertRecordWithId(
        "sc_instances", sc_id=sc_id, location_id=location_id, component_id=component_id)
    cur = con.cursor()

    return sc_id


def registerSC(con, loader_id, component_id, pkg_id, rootpath, pkg):
    cur = con.cursor()
    attrs = uPackaging.getPackageAttributes(pkg_id)

    if attrs.has_key("SC_GROUP_ID"):
        sc_group_id = int(attrs["SC_GROUP_ID"])
    else:
        sc_group_id = 1

    return registerSC2(con=con, name=pkg.package.name, version=pkg.version, user_id=2, sc_group_id=sc_group_id, loader_id=loader_id, location_id=location_id(pkg, rootpath), component_id=component_id)

__all__ = ["upgradeSC", "registerComponent", "location_id"]

