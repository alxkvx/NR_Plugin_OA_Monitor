#!/usr/bin/python

from xml.dom import minidom as dom

from uDLCommon import *


class Module:

    def __init__(self, name, version, description, source):
        self.name = name
        self.version = version
        self.description = description
        self.source = source
        self.packages = []
        self.packages_essentials = []
        self.packages_premium = []

    def add_package(self, node):
        self.packages.append(PkgInstallation(node=node))

    def add_package_essentials(self, node):
        for p in node.getElementsByTagName("PACKAGE"):
            self.packages_essentials.append(PkgInstallation(node=p))

    def add_package_premium(self, node):
        for p in node.getElementsByTagName("PACKAGE"):
            self.packages_premium.append(PkgInstallation(node=p))

    def unknown_element(self, node):
        raise Exception("%s: unknown element in %s" % (node.tagName, self.source))

    def __repr__(self):
        return "<Module %s-%s from '%s'>" % (self.name, self.version, self.source)

    def get_packages_to_install(self, config):
        if config.scale_down:
            return self.packages + self.packages_essentials
        return self.packages + self.packages_premium

xml_mapping = {
    "PACKAGE": Module.add_package,
    "ESSENTIALS": Module.add_package_essentials,
    "PREMIUM": Module.add_package_premium,
}


def readModuleFromDOM(doc, name):
    """
    :return: MDLReader.Module instance
    """
    root = doc.documentElement
    rv = Module(root.getAttribute("id"), root.getAttribute("version"), root.getAttribute("name"), name)
    content = doc.getElementsByTagName("CONTENT")
    if not content:
        raise Exception("No CONTENT module in %s" % name)

    for child in content[0].childNodes:
        if child.nodeType != dom.Node.ELEMENT_NODE or child.tagName == "TCONF":
            continue

        xml_mapping.get(child.tagName, Module.unknown_element)(rv, child)

    return rv


def readModule(fn):
    """
    :return: MDLReader.Module instance
    """
    doc = dom.parse(fn)
    return readModuleFromDOM(doc, fn)


def getInstalledModules(con):
    cur = con.cursor()

    # Platform goes first, and always consdired to be installed
    q = "SELECT mb.data, m.name FROM modules_body mb JOIN modules m ON (mb.module_id = m.module_id) WHERE "
    cur.execute(q + "m.name = 'Platform'")
    row = cur.fetchone()
    if not row:
        raise Exception("There is no Platform module")
    rv = [readModuleFromDOM(dom.parseString(str(row[0])), 'Platform')]

    cur.execute(q + "m.name != 'Platform' AND m.status != 'n'")

    return rv + [readModuleFromDOM(dom.parseString(str(row[0])), row[1]) for row in cur.fetchall()]
