__rcs_id__ = """$Id$"""

import re
import socket
import urllib2
from urlparse import urlparse


class URL:

    def __init__(self, url):
        self.url = url
        self.__parse_schema_host_port(url)
        self.__parse_user_password(url)
        self.noauth_url = self.__no_auth_url(url)

    def __parse_schema_host_port(self, url):
        p = urlparse(url.replace('#', ''))  # urlparse does not support # within net-location
        self.schema = p[0]
        netloc = p[1]
        host_with_port = netloc.split('@').pop().split(':')

        if len(host_with_port) == 1:
            map = {
                'ftp': 21,
                'http': 80,
                'https': 443,
                'file': None
            }

            host_with_port.append(map[self.schema])

        self.host, self.port = host_with_port

    def __parse_user_password(self, url):
        p = "%s://([^:@]*)(:(.*)){0,1}@.+" % self.schema
        m = re.match(p, url)

        if m is None:
            self.user = None
            self.password = None
            return

        # m.groups() is ('user', ':pass', 'pass')
        # 2nd and 3rd item can be None
        self.user, tmp, self.password = m.groups()

    def __no_auth_url(self, url):
        if self.user is not None:
            return re.sub("%s://[^/@]*@" % self.schema, self.schema + "://", url)
        else:
            return url


def try_connect(addr, timeout):
    try:
        s = socket.socket()
        s.settimeout(timeout)
        s.connect(addr)
    finally:
        s.close()


def config_urllib2(u):
    # only no proxy handler by default
    handlers = [urllib2.ProxyHandler({})]

    if u.schema.lower().startswith('http') and u.user is not None and u.password is not None:
        pm = urllib2.HTTPPasswordMgrWithDefaultRealm()
        pm.add_password(None, u.noauth_url, u.user, u.password)
        auth = urllib2.HTTPBasicAuthHandler(pm)
        handlers.append(auth)

    opener = urllib2.build_opener(*handlers)
    urllib2.install_opener(opener)


def check_url(url, timeout, ignore_http_codes=None):
    """Check if host from specified URL can be connected then try if path accessible"""
    u = URL(url)
    retval = None

    try:
        if u.schema != "file":
            try_connect((u.host, int(u.port)), timeout)
            config_urllib2(u)

        if u.schema == 'http':
            url_to_check = u.noauth_url
        else:
            url_to_check = u.url

        urllib2.urlopen(url_to_check)

    except urllib2.HTTPError, err:
        if not ignore_http_codes or err.code not in ignore_http_codes:
            retval = err

    except Exception, err:
        retval = err

    return retval
