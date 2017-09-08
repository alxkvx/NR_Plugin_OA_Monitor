import xml.dom.minidom as dom
import codecs

from uBuild import *


decode = codecs.getencoder("UTF-8")


def nodeTextContent(node):
    return ''.join([decode(text.data)[0] for text in node.childNodes if text.nodeType in (dom.Node.TEXT_NODE, dom.Node.CDATA_SECTION_NODE)])


def unsign_package(f):
    if type(f) == str:
        f = file(f)

    rv = ''
    to_skip = 0
    # read file without signatures. And we do not care if signature is not valid
    for line in f:
        if to_skip:
            to_skip -= 1
        elif line.startswith('-----BEGIN PGP SIGNED MESSAGE-----'):
            to_skip = 2
        elif line.startswith('-----BEGIN PGP SIGNATURE-----'):
            break
        elif line.startswith('- '):
            rv += line[2:]
        else:
            rv += line
    return rv


def get_package(f):
    not_signed_package = unsign_package(f)
    man_doc = dom.parseString(not_signed_package)

    pkg_node = man_doc.getElementsByTagName("PACKAGE")
    if not pkg_node:
        raise Exception, "Malformed package, no <PACKAGE> node"
    pkg_node = pkg_node[0]

    platform_node = man_doc.getElementsByTagName("PLATFORM")
    if not platform_node:
        raise Exception, "Malformed package, no <PLATFORM> node"
    platform_node = platform_node[0]

    platform = Platform(platform_node.getAttribute("opsys"), platform_node.getAttribute(
        "osrel"), platform_node.getAttribute("arch"))

    is_single = False
    is_custom = False
    attr_nodes = man_doc.getElementsByTagName("ATTRIBUTE")

    for node in attr_nodes:
        if node.getAttribute('name') == "COMPONENT.SINGLE" and node.getAttribute('value') == "yes":
            is_single = True

        if node.getAttribute('name').upper() == "CUSTOMER":
            is_custom = True

    package = Package(pkg_node.getAttribute("name"), pkg_node.getAttribute("type"), platform, is_single, is_custom)

    distfile_node = man_doc.getElementsByTagName("DISTFILE")
    content = None
    if distfile_node:
        content_node = man_doc.getElementsByTagName("CONTENT")
        filemap = {}
        bin = None
        if content_node:
            bin = content_node[0].getAttribute("bin")
            for node in [x for x in content_node[0].childNodes if x.nodeType == x.ELEMENT_NODE and x.tagName == u'FILE']:
                filemap[node.getAttribute('name')] = node.getAttribute('md5')
        content = BuiltPackageContent(
            distfile_node[0].getAttribute('filename'), distfile_node[0].getAttribute("md5"), filemap)
        content.bin = bin

    rv = BuiltPackage(package, pkg_node.getAttribute("version"), content)

    return rv


__all__ = ["unsign_package", "get_package"]
