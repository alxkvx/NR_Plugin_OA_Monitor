import socket
import os
import ConfigParser

import uLogging
import uUtil
import uOSCommon
from uConst import Const

def findDBConfig(rootpath):
    default_rv = {
        'database_host': socket.gethostname(),
        'database_name': 'oss',
        'database_port': '5432',
        'dsn': 'oss',
        'dsn_login': 'oss',
        'dsn_passwd': '',
        'database_type': '',
        'kernel_priv_key': ''
    }

    etc_dir = os.path.join(rootpath, "etc")
    kernel_conf_path = os.path.join(etc_dir, "Kernel.conf")
    if Const.isWindows():
        default_rv['database_type'] = 'MSSQL'
        if os.path.isfile(kernel_conf_path):
            kernel_conf = file(kernel_conf_path)
            pp = uUtil.readPropertiesFile(kernel_conf)
            kernel_conf.close()
            uLogging.debug('read kernel config from %s' % kernel_conf_path)
            default_rv['kernel_priv_key'] = pp['kernel_priv_key']
            default_rv['dsn_passwd'] = pp['dsn_passwd']
            default_rv['dsn_login'] = pp['dsn_login']
        else:
            uLogging.debug('no db config, taking defaults. valid for fresh install only!')
        return default_rv
    else:
        odbc_ini_path = os.path.join(etc_dir, "odbc.ini")

        if not os.path.isfile(kernel_conf_path) or not os.path.isfile(odbc_ini_path):
            uLogging.debug('no db config, taking defaults. valid for fresh install only!')
            default_rv['database_type'] = 'PGSQL'
            return default_rv

        kernel_conf = file(kernel_conf_path)
        pp = uUtil.readPropertiesFile(kernel_conf)
        kernel_conf.close()
        uLogging.debug('read kernel config from %s' % kernel_conf_path)

        odbc = ConfigParser.ConfigParser()
        odbc.read(odbc_ini_path)
        uLogging.debug('read odbc config from %s' % odbc_ini_path)

        dsnname = pp["dsn"]
        rv = {'database_host': odbc.get(dsnname, "Servername"), 'database_port': odbc.get(dsnname, "Port"), 'database_name': odbc.get(dsnname, "Database"),
              'dsn': dsnname, 'dsn_login': pp["dsn_login"], 'dsn_passwd': pp['dsn_passwd'], 'database_type': 'PGSQL', 'kernel_priv_key': pp['kernel_priv_key']}

        # we are here because ODBC DSN configured and actually the type must be rv['type'] = 'ODBC'
        # but uSysDB doesn't know how to deal with 'ConcatOperator' and 'nowfun' in this case
        return rv


def findAgentConfig(rootpath):
    commip = None
    kernel_pub_key = ""
    encryption_key = ""
    system_password = ""
    pleskd_endpoint_port = '8352'

    etc_dir = os.path.join(rootpath, "etc")
    conf_path = os.path.join(etc_dir, "pleskd.props")

    if os.path.isfile(conf_path):
        conf = file(conf_path)
        pp = uUtil.readPropertiesFile(conf)
        conf.close()
        commip = pp.get('communication.ip', commip)
        kernel_pub_key = pp.get('kernel_pub_key', kernel_pub_key)
        encryption_key = pp.get('encryption_key', encryption_key)
        system_password = pp.get('passwd', system_password)
        pleskd_endpoint_port = pp.get('orb.endpoint.port', pleskd_endpoint_port)
        uLogging.debug('read agent config from %s' % conf_path)
    else:
        uLogging.debug('no agent config, taking defaults. valid for fresh install only!')

    rv = {'commip': commip,
          'kernel_pub_key': kernel_pub_key,
          'encryption_key': encryption_key,
          'system_password': system_password,
          'pleskd_endpoint_port': pleskd_endpoint_port
          }
    return rv


class Config:

    def __init__(self, batch=False, reinstall=False, verbose=False, migrate=None, modules=None, yum_repo_url='', distrib_content_path='', rootpath=None, yum_repo_proxy_url='', **kwargs):
        if rootpath is None:
            rootpath = uOSCommon.getUOSModule().getDefaultRootpath()

        # initialize default values
        agentConfig = findAgentConfig(rootpath)
        if agentConfig['commip']:
            self.communication_ip = agentConfig['commip']
        self.kernel_pub_key = agentConfig['kernel_pub_key']
        self.encryption_key = agentConfig['encryption_key']
        self.system_password = agentConfig['system_password']
        self.pleskd_endpoint_port = agentConfig['pleskd_endpoint_port']

        dbConfig = findDBConfig(rootpath)
        self.kernel_priv_key = dbConfig['kernel_priv_key']
        self.database_host = dbConfig['database_host']
        self.database_name = dbConfig['database_name']
        self.database_port = dbConfig['database_port']
        self.dsn = dbConfig['dsn']
        self.dsn_login = dbConfig['dsn_login']
        self.dsn_passwd = dbConfig['dsn_passwd']
        self.database_type = dbConfig['database_type']
        self.database_odbc_driver = None

        self.hostname = socket.gethostname()
        self.rootpath = rootpath
        self.kernel_endpoint_port = '8354'
        self.username = 'admin'
        self.password = '1q2w3e'

        self.openapi_host = "127.0.0.1"  # work in only-action mode only
        self.openapi_proto = 'http'
        self.openapi_port = '8440'
        self.openapi_user = None
        self.openapi_password = None
        self.force = False

        self.log_file = uOSCommon.getUOSModule().get_default_log_file()
        self.log_file_rotation = False
        self.verbose = False
        self.warning_duplicates = False
        self.cache = None
        self.actions_onerror = None
        self.slave_upgrade_threads = None
        self.simple_ui = False

        # customizable config values:
        self.batch = batch
        self.reinstall = reinstall
        self.migrate = migrate
        self.modules = modules
        self.yum_repo_url = yum_repo_url
        self.yum_repo_proxy_url = yum_repo_proxy_url
        self.distrib_content_path = distrib_content_path    # make sense for install only. for update there may be multiple distribs
        self.platform = None
        self.update_name = None

        # os-specific default parameters
        uOSCommon.getUOSModule().initDefaultProps(self)

        # process keyword args
        self.__dict__.update(kwargs)


__all__ = ["Config"]
