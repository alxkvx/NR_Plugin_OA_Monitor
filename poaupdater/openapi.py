# $Id$

import xmlrpclib as xmlrpc
import base64
import socket
import errno
import sys
import time

import uLogging
import uDialog
import uAction
import uUtil
import uPEM
import uSysDB


# The authorization performed via base64 encoded http headers, which should not contain line breaks.
# But base64.encodestring() adds linebreaks "\n" if line is too long. Limits:
#   base64.MAXLINESIZE = 76
#   base64.MAXBINSIZE = (MAXLINESIZE//4)*3  # => 57
# This is for avoiding unwanted line breaks after encoding
base64.MAXLINESIZE = 268
base64.MAXBINSIZE = (base64.MAXLINESIZE//4)*3  # => 201


class OpenAPISettings:
    host = '127.0.0.1'
    port = 8440
    user = None
    password = None
    https = False
    timeout = None

    def __str__(self):
        if self.user is None:
            return self.url()
        else:
            return "%s@%s" % (self.user, self.url())

    def url(self):
        if self.https:
            proto = 'https'
        else:
            proto = 'http'
        return '%s://%s:%s' % (proto, self.host, self.port)


class ConnectionRefused(Exception):

    def __init__(self):
        Exception.__init__(self, "Connection attempt to Open API refused. Please check if system is running")

_settings = OpenAPISettings()


def init(host=_settings.host, port=_settings.port, user=_settings.user, proto=_settings.https and 'https' or 'http', password=_settings.password, timeout=_settings.timeout):
    global _settings
    _settings.host = host
    _settings.port = port
    _settings.user = user
    if proto == 'http':
        _settings.https = False
    elif proto == 'https':
        _settings.https = True
    else:
        raise Exception("%s: invalid or not supported protocol" % proto)
    _settings.password = password
    _settings.timeout = timeout
    uLogging.debug("OpenAPI init, settings: %s" % uUtil.stipPasswords(vars(_settings)))

# used by installer & updater
def initFromEnv(config):
    port = config.openapi_port or '8440'
    proto = 'http'
    user = password = None

    con = uSysDB.connect()
    if uSysDB.table_exist(con, "openapi_config"):
        cur = con.cursor()
        cur.execute("SELECT require_auth, use_ssl FROM openapi_config")
        row = cur.fetchone()

        if row:
            if row[1] == 'y':
                proto = 'https'

            if row[0] == 'y':
                pleskd_props = uPEM.getPleskdProps()
                user = pleskd_props['login']
                password = pleskd_props['passwd']

                if not user and not password:
                    raise Exception("OpenAPI authentication is enabled, but valid credentials were not found.")

    init(host=config.communication_ip, port=port, user=user, proto=proto, password=password)

def isSSLEnabled():
    return _settings.https

class Transport(xmlrpc.SafeTransport):

    def __init__(self, config):
        if '__init__' in dir(xmlrpc.SafeTransport):
            xmlrpc.SafeTransport.__init__(self)
        self.config = config
        # do not have any idea what does that mean:
        self._use_datetime = 0

    def make_connection(self, host):
        TransportClass = self.config.https and xmlrpc.SafeTransport or xmlrpc.Transport
        connection = TransportClass.make_connection(self, host)

        try:
            # Get HTTP(S)Connection instance and set timeout on it
            try:
                # Python <= 2.6 - connection is HTTP(S) instance
                http_conn = connection._conn
            except AttributeError:
                # Python >= 2.7 - connection is HTTP(S)Connection instance
                http_conn = connection

            http_conn.timeout = self.config.timeout
            if http_conn.sock is not None:
                http_conn.sock.settimeout(self.config.timeout)
        except AttributeError:
            uLogging.warn("Failed to set OpenAPI connection socket timeout - operation not supported")

        return connection

    def send_host(self, connection, host):
        if self.config.https:
            return xmlrpc.SafeTransport.send_host(self, connection, host)
        else:
            return xmlrpc.Transport.send_host(self, connection, host)

    def send_content(self, connection, request_body):
        connection.putheader("Content-Type", "text/xml")
        connection.putheader("Content-Length", str(len(request_body)))
        if self.config.user is not None:
            connection.putheader("Authorization", "Basic %s" %
                                 base64.encodestring("%s:%s" % (self.config.user, self.config.password)).strip())
        connection.endheaders()
        if request_body:
            connection.send(request_body)


class OpenAPIError (Exception):
    def __init__(self, resp):
        msg = resp['error_message'].encode('ascii', 'backslashreplace')
        Exception.__init__(self, msg)
        self.error_message = msg
        self.module_id = resp.get('module_id', 'unknown module')
        self.extype_id = resp.get('extype_id', 'unknown exception type')
        self.properties = resp.get('properties', dict())


def _transform_params(params, func=None):
    if type(params) == list:
        return [_transform_params(p, func) for p in params if p is not None]
    elif type(params) != dict:
        return params

    rv = {}
    for k, v in params.iteritems():
        if type(v) in (dict, list):
            rv[k] = _transform_params(v, func)
        elif v is not None:
            rv[k] = func and func(k,v) or v
    return rv

def _mask_params(params):
    rv = {}
    for k, v in params.iteritems():
        rv[k] = v
        if k == 'password' or k == 'pwd' or k == 'passwd' or k == "license_id" or k == "aps_token":
            rv[k] = '***'
    return rv


class _Method:

    def __init__(self, api, name):
        self.name = name
        self.api = api

    def __getattr__(self, name):
        return _Method(self.api, '%s.%s' % (self.name, name))

    def __call__(self, **kwds):
        params = _transform_params(kwds)
        if self.api.txn_id is not None:
            params['txn_id'] = self.api.txn_id
        try:
            uLogging.debug("call method %s(%s)" % (self.name, _mask_params(params)))
            resp = getattr(self.api.server, self.name)(params)
            uLogging.debug("return %s" % (_transform_params(resp, lambda k, v: uUtil.is_secret_key(k) and "***" or v)))
        except socket.error, e:
            if type(e.args) == tuple and e.args[0] == errno.ECONNREFUSED:
                raise ConnectionRefused()
            else:
                raise

        if resp['status'] != 0:
            # POA transaction is closed on error
            self.api.txn_id = None
            raise OpenAPIError(resp)
        if resp.get('signature'):
            return resp['signature']
        return resp.get('result', None)


class OpenAPI:

    def __init__(self, settings=_settings, namespace=None):
        self.settings = settings
        self.server = xmlrpc.ServerProxy(self.settings.url(), transport=Transport(self.settings))
        self.txn_id = None
        self.namespace = namespace

        # shut up pychecker in most cases
        self.pem = _Method(self, 'pem')

    def begin(self, txn_id=None, request_id=None):
        params = {}
        if txn_id is not None:
            params['txn_id'] = txn_id
        if request_id is not None:
            params['request_id'] = request_id
        try:
            resp = self.server.txn.Begin(params)
        except socket.error, e:
            if type(e.args) == tuple and e.args[0] == errno.ECONNREFUSED:
                raise Exception("Connection attempt to Open API refused. Please check if system is running")
            else:
                raise

        if resp['status'] != 0:
            raise OpenAPIError(resp)
        self.txn_id = resp['result']['txn_id']

    def beginRequest(self, request_id_cand='req', prefix_gen=lambda: "%.6f" % time.time()):
        tc = 0
        request_id = None
        prefix = ''
        if prefix_gen:
            prefix = prefix_gen()

        while request_id is None:
            try:
                begin_request_id = prefix + request_id_cand
                # 'request_id' is used for composing task group name in TaskManager controller;
                # task group name is stored in data base in column with type 'character varying(256)'
                begin_request_id = begin_request_id[:256]
                self.begin(request_id=begin_request_id)
                request_id = begin_request_id
            except OpenAPIError, e:
                if e.module_id == 'OpenAPI' and e.extype_id == 3001:
                    tc += 1
                    prefix = '%d_' % tc
                else:
                    raise
        return request_id

    def commit(self):
        if self.txn_id is None:
            raise Exception("No transaction in progress")
        try:
            resp = self.server.txn.Commit({'txn_id': self.txn_id})
        except socket.error, e:
            if type(e.args) == tuple and e.args[0] == errno.ECONNREFUSED:
                raise Exception("Connection attempt to Open API refused. Please check if system is running")
            else:
                raise

        if resp['status'] != 0:
            # POA transaction is closed on error
            self.api.txn_id = None
            raise OpenAPIError(resp)

        self.txn_id = None

    def rollback(self):
        if self.txn_id is None:
            raise Exception("No transaction in progress")
        self.server.txn.Rollback({'txn_id': self.txn_id})
        self.txn_id = None

    def __getattr__(self, name):
        if self.namespace is None:
            nm = name
        else:
            nm = '%s.%s' % (self.namespace, name)

        return _Method(self, nm)

    def __repr__(self):
        if self.txn_id is None:
            txn = "no transaction"
        else:
            txn = "txn '%s'" % self.txn_id

        return "<Open API at %s, %s>" % (self.settings, txn)


def waitRequestComplete(request_id, operation, doWait):
    waiting = False
    api = OpenAPI()
    default_tried = False
    sys_stderr_written = False
    while True:
        request_status = api.pem.getRequestStatus(request_id=request_id)
        if request_status['request_status'] == 1:
            if not waiting:
                uLogging.info("%s (request %s) is not completed yet.", operation, request_id)
        elif request_status['request_status'] == 2:
            uLogging.err("%s operation failed", operation)
            for e in request_status['status_messages']:
                uLogging.err('%s', e)
            doWait = False
            waiting = False
        else:
            if sys_stderr_written:
                sys.stderr.write('\n')
            uLogging.info("%s finished successfully", operation)
            return True

        if doWait and not waiting:
            uLogging.info("Waiting for %s completion (Interrupt to abort)", operation)
            waiting = True
        if waiting:
            try:
                sys.stderr.write('.')
                sys_stderr_written = True
                time.sleep(3)
            except KeyboardInterrupt, e:
                doWait = False
        if doWait:
            continue

        default_error_action = uAction.get_default_error_action()
        if default_error_action and not default_tried:
            action = default_error_action
            default_tried = True
        else:
            action = uDialog.askVariants('What should I do', '(A)bort', ['(A)bort', '(R)echeck', '(I)gnore', '(W)ait'])
        if action in ('(A)bort', 'abort'):
            raise Exception('%s failed' % operation)
        elif action in ('(I)gnore', 'ignore'):
            if sys_stderr_written:
                sys.stderr.write('\n')
            return False
        elif action in ('(W)ait'):
            doWait = True
        elif action in ('(R)echeck', 'retry'):
            waiting = doWait = False

    if sys_stderr_written:
        sys.stderr.write('\n')
