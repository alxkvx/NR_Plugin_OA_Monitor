#!/usr/bin/python

MN_pkg = ('chief', 'sc')


class NativePackage:

    def __init__(self, name, nptype, host, where, name_x64=None):
        self.name = name  # Filename
        self.name_x64 = name_x64 or name
        self.type = nptype or "rpm"
        self.host = host
        self.where = where
        self.package_name = None


class PkgInstallation:

    def _node_init__(self, node):
        self.name = node.getAttribute("name")
        self.ctype = node.getAttribute("ctype")
        where = node.getAttribute("host")
        if where == "mn" or where == "ui":
            self.where_pkg = MN_pkg
        elif where == "no":
            self.where_pkg = None
        else:  # upgrade
            self.where_pkg = (self.name, self.ctype)

    def _pkg_init__(self, name, ctype, where):
        self.name = name
        self.ctype = ctype
        self.where_pkg = where

    def __init__(self, node=None, name=None, ctype=None, where=None):
        if node:
            PkgInstallation._node_init__(self, node)
        else:
            PkgInstallation._pkg_init__(self, name, ctype, where)

    def node(self, doc):
        rv = doc.createElement("PACKAGE")
        rv.setAttribute("name", self.name)
        rv.setAttribute("ctype", self.ctype)
        if self.where_pkg == ('chief', 'sc'):
            rv.setAttribute("host", "mn")
        elif self.where_pkg == (self.name, self.ctype):
            rv.setAttribute("host", "upgrade")
        else:
            rv.setAttribute("host", "no")

        return rv


def parseNativePackageNode(node):
    host = node.getAttribute("host") or "all"
    name = node.getAttribute("name")
    name_x64 = node.getAttribute("name_x64") or name
    ptype = node.getAttribute("type") or "rpm"
    where = None  # None will mean everywhere.

    return NativePackage(name, ptype, host, where, name_x64)

__all__ = ["PkgInstallation", "NativePackage", "MN_pkg", "parseNativePackageNode"]
