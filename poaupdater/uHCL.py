__rcs_id__ = """$Id$"""

__pychecker__ = 'unusednames=__rcs_id__,dummy maxargs=16'

import sys
import os
import tempfile
from xml.dom import minidom as dom
import re
import base64
import time
import urllib2
import ssl
try:
    import json
except ImportError:
    import simplejson as json

import uPEM
import uSysDB
import uLogging
import uUtil
from uConst import Const


def bool_str(val):
    if val:
        return "yes"
    else:
        return "no"


def setOptionalAttr(elem, name, value):
    if value is not None:
        elem.setAttribute(name, value)


def _get_element_value(el, name):
    result = ""
    targets = el.getElementsByTagName(name)
    if len(targets) == 0:
        return result
    for node in targets[0].childNodes:
        if node.nodeType == node.TEXT_NODE:
            result += str(node.data)
    return result


def _get_hcl_property_value(prop_elem, name):
    for node in prop_elem.getElementsByTagName(name)[0].childNodes:
        if node.nodeType == node.TEXT_NODE:
            return str(node.data).strip(" \t\r\n").decode("hex")
    return ""


def _get_key_fingerprint(key, data, host):
    (keyfd, keyfile) = tempfile.mkstemp()
    try:
        os.write(keyfd, key)
    finally:
        os.close(keyfd)

    (digest, err, status) = uUtil.readCmdExt(["/bin/sh", "-c", "openssl dgst -sha256 -hex -sign %s" % keyfile], data)
    os.remove(keyfile)

    if status != 0:
        raise Exception("Failed to sign host %s JWT header. %s" % (host.host_id, err))

    digest = digest.strip()
    spaceidx = digest.find(" ")
    if spaceidx <> -1:
        digest = digest[spaceidx + 1:]

    return digest


def getHostCertificateDigest(host):
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("select sn_certificate from hosts where host_id = %s", host.host_id)
    row = cur.fetchone()
    if not(row and len(row[0]) > 0):
        raise Exception("Failed to get SN certificate for host %s from database." % (str(host.host_id)))

    (keyfd, keyfile) = tempfile.mkstemp()
    try:
        os.write(keyfd, row[0])
    finally:
        os.close(keyfd)

    cmd = "openssl x509 -in %s -pubkey -noout | grep -v '--' - | tr -d '\\n' | base64 -d | openssl dgst -sha256 -hex" % keyfile
    (digest, err, status) = uUtil.readCmdExt(["/bin/sh", "-c", cmd])

    os.remove(keyfile)

    digest = digest.strip()
    spaceidx = digest.find(" ")
    if spaceidx <> -1:
        digest = digest[spaceidx + 1:]
    return digest


def getHostAuthToken(host, config, issuer):
    kernel_pkey = config.kernel_priv_key
    pkey_formatted = "-----BEGIN RSA PRIVATE KEY-----\n"
    i = 0
    while i < len(kernel_pkey):
        pkey_formatted += kernel_pkey[i:i+64] + "\n"
        i += 64
    pkey_formatted += "-----END RSA PRIVATE KEY-----"
    host_ip = uPEM.getHostCommunicationIP(host.host_id)
    digest = getHostCertificateDigest(host)
    jose = { "alg": "RS256", "typ": "JWT" }
    iat = int(time.mktime(time.localtime()))
    token = {
        "iss": issuer,
        "aud": digest,
        "nbf": iat,
        "iat": iat,
        "exp": iat + 3600,
        "hcl_host_ip": host_ip
    }
#    uLogging.debug("JOSE: " + json.dumps(jose))
#    uLogging.debug("Token: " + json.dumps(token))
    header = str(base64.urlsafe_b64encode(json.dumps(jose))).rstrip("=") + "." \
             + str(base64.urlsafe_b64encode(json.dumps(token))).rstrip("=")
    signature = _get_key_fingerprint(pkey_formatted, header, host)
    signature = signature.decode("hex")
    return header + "." + str(base64.urlsafe_b64encode(signature)).rstrip("=")


def readHCLOperationResult(resp):
    document = dom.parseString(resp)
    result = {}
    for prop_elem in document.getElementsByTagName("property"):
        result[_get_hcl_property_value(prop_elem, "name")] = _get_hcl_property_value(prop_elem, "value")
    return result


def readHCLOperationError(resp):
    result = { "module": "", "type": "", "code": -1, "message": ""}
    try:
        document = dom.parseString(resp)
    except Exception:
        return result
    errors = document.getElementsByTagName("operations_error")
    if len(errors) == 0:
        return result
    err_node =errors[0]
    result["module"] = _get_element_value(err_node, "module")
    result["type"] = _get_element_value(err_node, "type")
    result["message"] = _get_element_value(err_node, "message")
    result["code"] = int(_get_element_value(err_node, "code"))
    return result


class Request:

    def __init__(self, host_id=None, user=None, group=None, auto_export=True):
        self.__host_id = host_id
        self.auto_export = auto_export
        self.__brokenSchema = uPEM.get_major_version() == "6.5"
        self.__delegated_hosts = set()
        self.__issuer = None

        self.clear()
        self.set_creds(user, group)

    def clear(self):
        impl = dom.getDOMImplementation()
        dt = impl.createDocumentType("HCL", "HCL 1.0", "HCL")
        self.__document = impl.createDocument("HCL", "HCL", dt)
        self.__declare = self.__document.createElement("DECLARE")
        self.__document.documentElement.appendChild(self.__declare)
        self.__perform = self.__document.createElement("PERFORM")
        self.__document.documentElement.appendChild(self.__perform)

    def toxml(self):
        data = self.__HCLHeader
        return data + self.__document.documentElement.toxml()

    def set_creds(self, user=None, group=None):
        if user is not None:
            self.set("default_user", user)
        if group is not None:
            self.set("default_group", group)

    def __setVarAttr(self, elem, attname, varname):
        if varname is None:
            return

        if self.auto_export:
            self.export(varname)

        elem.setAttribute(attname, varname)

    def edit(self, path, action):
        elem = self.__document.createElement("EDIT")
        elem.setAttribute("path", path)
        elem.appendChild(self.__document.createCDATASection(action))
        self.__perform.appendChild(elem)

    def mkfile(self, path, contents, owner="${default_user}", group="${default_group}", perm=None, overwrite=True):
        elem = self.__document.createElement("CREATEFILE")
        elem.setAttribute("path", path)
        elem.setAttribute("owner", owner)
        elem.setAttribute("group", group)
        setOptionalAttr(elem, "perm", perm)
        elem.setAttribute("overwrite", bool_str(overwrite))
        text = self.__document.createCDATASection(contents)
        elem.appendChild(text)
        self.__perform.appendChild(elem)

    def mkfileb64(self, path, contents, owner="${default_user}", group="${default_group}", perm=None, overwrite=True):
        import base64
        elem = self.__document.createElement("CREATEFILEB64")
        elem.setAttribute("path", path)
        elem.setAttribute("owner", owner)
        elem.setAttribute("group", group)
        setOptionalAttr(elem, "perm", perm)
        elem.setAttribute("overwrite", bool_str(overwrite))
        text = self.__document.createCDATASection(base64.encodestring(contents))
        elem.appendChild(text)
        self.__perform.appendChild(elem)

    def set(self, name, value):
        elem = self.__document.createElement("SET")
        elem.setAttribute("var", name)
        if self.__brokenSchema:
            elem.setAttribute("attrValue", value)
        else:
            elem.setAttribute("value", value)
        self.__perform.appendChild(elem)

    def export(self, name, value=None, transient=False):
        elem = self.__document.createElement("VAR")
        elem.setAttribute("name", name)
        if value is not None:
            if self.__brokenSchema:
                elem.setAttribute("attrValue", value)
            else:
                elem.setAttribute("value", value)
        elem.setAttribute("transient", bool_str(transient))

        self.__declare.appendChild(elem)

    def command(self, command, args=None, stdout=None, stderr=None, cwd='/', stdin=None, user="${default_user}", group="${default_user}", valid_exit_codes=None, retvar=None):
        if args is None:
            args = ["${default_shell_name}", "${shell_cmd_switch}", command]
            command = "${default_shell_name}"
        elem = self.__document.createElement("EXEC")
        elem.setAttribute("command", command)
        elem.setAttribute("workdir", cwd)
        setOptionalAttr(elem, "user", user)
        setOptionalAttr(elem, "group", group)
        self.__setVarAttr(elem, "outvar", stdout)
        self.__setVarAttr(elem, "errvar", stderr)
        self.__setVarAttr(elem, "retvar", retvar)
        if valid_exit_codes is not None:
            elem.setAttribute("valid_exit_codes", ','.join([str(x) for x in valid_exit_codes]))
        for arg in args:
            arg_elem = self.__document.createElement("ARG")
            arg_elem.setAttribute("value", arg)
            elem.appendChild(arg_elem)
        if stdin is not None:
            in_elem = self.__document.createCDATASection(stdin)
            elem.appendChild(in_elem)

        self.__perform.appendChild(elem)

    def fetch(self, srcfile, urls, dstfile=None, dstvar=None):
        elem = self.__document.createElement("FETCH")
        elem.setAttribute("srcfile", srcfile)

        for url in urls:
            lelem = self.__document.createElement("LOCATION")
            lelem.setAttribute("value", url)
            elem.appendChild(lelem)

        if dstfile is None:
            elem.setAttribute("dstfile", "")
        else:
            elem.setAttribute("dstfile", dstfile)

        self.__setVarAttr(elem, "tmpfilevar", dstvar)

        self.__perform.appendChild(elem)

    def mkdir(self, directory):
        elem = self.__document.createElement("MKDIR")
        elem.setAttribute("path", directory)
        self.__perform.appendChild(elem)

    def rm(self, path):
        elem = self.__document.createElement("RM")
        elem.setAttribute("path", path)
        self.__perform.appendChild(elem)

    def move(self, src, dst):
        elem = self.__document.createElement("MOVE")
        elem.setAttribute("src", src)
        elem.setAttribute("dst", dst)
        elem.setAttribute("overwrite", "yes")
        self.__perform.appendChild(elem)

    def copy(self, src_dir, dst_dir):
        elem = self.__document.createElement("COPY")
        elem.setAttribute("src", src_dir)
        elem.setAttribute("dst", dst_dir)
        self.__perform.appendChild(elem)

    def chown(self, path, user="${default_user}", group="${default_group}", recursive=True):
        elem = self.__document.createElement("CHOWN")
        elem.setAttribute("path", path)
        elem.setAttribute("recursive", bool_str(recursive))
        elem.setAttribute("user", user)
        elem.setAttribute("group", group)
        self.__perform.appendChild(elem)

    def chmod(self, path, perm, recursive=False, action="set"):
        elem = self.__document.createElement("CHMOD")
        elem.setAttribute("path", path)
        elem.setAttribute("perm", perm)
        elem.setAttribute("action", action)
        elem.setAttribute("recursive", bool_str(recursive))
        self.__perform.appendChild(elem)

    def readfile(self, path, content, binary=False):
        if binary:
            cmd = "READB64"
        else:
            cmd = "READ"
        elem = self.__document.createElement(cmd)
        elem.setAttribute("path", path)
        self.__setVarAttr(elem, "variable", content)

        self.__perform.appendChild(elem)

    def extract(self, archive, rootpath):
        elem = self.__document.createElement("EXTRACT")
        elem.setAttribute("filename", archive)
        elem.setAttribute("rootpath", rootpath)

        self.__perform.appendChild(elem)

    def uptime(self, uptimevar="${uptime}", currenttimevar="${currenttime}"):
        elem = self.__document.createElement("UPTIME")
        elem.setAttribute("uptimevar", uptimevar)
        elem.setAttribute("current_time", currenttimevar)

        self.__perform.appendChild(elem)

    def ipadd(self, ip, netmask, ifname):
        elem = self.__document.createElement("IPADD")
        elem.setAttribute("ip", ip)
        elem.setAttribute("netmask", netmask)
        elem.setAttribute("ifname", ifname)

        self.__perform.appendChild(elem)

    def perform(self, host_id=None):
        host_id = host_id or self.__host_id
        if host_id is None:
            raise Exception("host_id not defined")
        con = uSysDB.connect()
        host = uPEM.getHost(host_id)
        return self.performRaw(host)

    def performCompat(self, host_id=None):
        host_id = host_id or self.__host_id
        if host_id is None:
            raise Exception("host_id not defined")
        con = uSysDB.connect()
        host = uPEM.getHost(host_id)
        try:
            return self.performRaw(host)
        except uUtil.ExecFailed:
            return self.send(host)

    def performRaw(self, host):
        if Const.isOsaWinPlatform(host.platform.os):
            shell = "cmd.exe"
            cmd_switch = "/C"
        else:
            shell = "/bin/sh"
            cmd_switch = "-c"

        self.export("default_shell_name", shell, True)
        self.export("shell_cmd_switch", cmd_switch, True)
        try:
            return self.__performHCLRequest(host, self.__document)
        except uUtil.ExecFailed, e:
            # Notation "ex_type_id:'103'" in stderr it is a sign that exception OBJECT_NOT_EXIST is raised
            # (from modules/platform/u/EAR/poakernel-public/Common.edl).
            # We need to retry HCL request because the reason may be an outdated CORBA cache.
            # Cache will be invalidated in this case, so repeated request will pass
            # (modules/platform/cells/pem_client/cpp/Naming/Naming.cpp).
            deprecated_cache_error_pattern = re.compile("OBJ_ADAPTER|OBJECT_NOT_EXIST|ex_type_id:'103'")
            if re.search(deprecated_cache_error_pattern, e.err):
                uLogging.debug("HCL request exec failed. Retrying...")
                return self.__performHCLRequest(host, self.__document)
            else:
                raise

    def transfer(self, src_host_id, path_from, path_to):
        elem = self.__document.createElement("TRANSFER")
        elem.setAttribute("src_host_id", src_host_id)
        self.__delegated_hosts.add(src_host_id)

        dir_el = self.__document.createElement("PATHSUBST")
        dir_el.setAttribute("what", path_from)
        dir_el.setAttribute("to", path_to)
        elem.appendChild(dir_el)

        self.__perform.appendChild(elem)

    __HCLHeader = """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
    <!DOCTYPE HCL PUBLIC "HCL" "HCL 1.0">"""

    def __performHCLRequest(self, host, hcl):
        fd, filename = tempfile.mkstemp()
        f = os.fdopen(fd, "w")
        if isinstance(hcl, dom.Document):
            # stupid toxml method does not include <DOCTYPE> to output.
            # do do it manually, and write only documentElement
            f.write(self.__HCLHeader)
            # do no pretty printing, because it can damage HCL request with whitespaces and newlines
            # (e.g. content of files to be created)
            hcl.documentElement.writexml(writer=f)
        else:
            f.write(hcl)
        f.close()

        if host.pleskd_id:
            out = uPEM.readCtl('pleskd_ctl', ['-s', str(host.pleskd_id), 'processHCL', filename, str(host.host_id)])
        else:
            out = uPEM.readCtl('pleskd_ctl', ['-H', str(host.host_id), 'processHCL', filename, str(host.host_id)])

        prop_values = out.split('\0')
        rv = dict(zip(prop_values[::2], prop_values[1::2]))

        # File is not removed if execCommand fail, and it is ok: this file could
        # be needed to understand what went wrong.
        os.unlink(filename)

        return rv

    def send(self, host):
        from poaupdater import uConfig
        status_code = -1
        resp_out = ""
        if self.__issuer is None:
            self.__issuer = getHostCertificateDigest(uPEM.getHost(1))
        config = uConfig.Config()
        authToken = getHostAuthToken(host, config, self.__issuer)
        host_ip = uPEM.getHostCommunicationIP(host.host_id)
        uLogging.debug("Send REST HCL request to host %s (%s) ..." % (str(host.host_id), host_ip))
        # uLogging.debug("%s" % self.toxml())
        headers = {"Authorization": authToken, "X-Auth-Host%s" % str(host.host_id) : authToken}
        for d_host_id in self.__delegated_hosts:
            if d_host_id != host.host_id:
                headers["X-Auth-Host%s" % str(d_host_id)] = getHostAuthToken(uPEM.getHost(d_host_id), config, self.__issuer)
        http_req = urllib2.Request("https://%s:8352/process" % host_ip, headers=headers, data=None)
        http_req.get_method = lambda: 'POST'
        try:
            if sys.version_info >= (2, 7, 9):
                resp = urllib2.urlopen(http_req, context=ssl._create_unverified_context(), data=self.toxml())
            else:
                resp = urllib2.urlopen(http_req, data=self.toxml())
            resp_out = resp.read()
            try:
                status_code = resp.getcode()
            except AttributeError:
                status_code = resp.code
        except urllib2.HTTPError, error:
            status_code = error.code
            resp_out = error.read()
        except urllib2.URLError, error:
            raise uUtil.ExecFailed("", 1, str(error))

        if status_code == 200:
            # uLogging.debug("Request output:\n%s" % resp_out)
            return readHCLOperationResult(resp_out)

        uLogging.err("Request error:\n%s" % resp_out)
        op_error = readHCLOperationError(resp_out)
        raise uUtil.ExecFailed("", op_error["code"], op_error["message"])


def runHCLCmd(hostId, commandText):
    uLogging.debug(commandText)
    con = uSysDB.connect()
    host = uPEM.getHost(hostId)
    commandText = commandText.replace("${", '$${') #for more details see following issue https://jira.int.zone/browse/POA-109131
    rq = Request(user='root', group='root')
    rq.command(commandText, stdout='stdout', stderr='stderr', valid_exit_codes=[0])
    if Const.isOsaWinPlatform(host.platform.os):
        rq.export("default_shell_name", "cmd.exe", True)
        rq.export("shell_cmd_switch", "/C", True)
    else:
        rq.export("default_shell_name", "/bin/sh", True)
        rq.export("shell_cmd_switch", "-c", True)
    rqRv = None
    try:
        rqRv = rq.send(host)
    except uUtil.ExecFailed:
        rqRv = rq.performRaw(host)
    o = rqRv["stdout"]
    if o: uLogging.debug(o)
    return o

