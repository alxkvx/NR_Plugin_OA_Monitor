import uHCL
import uPackaging
import uPEM
import openapi

def _getPuiHosts():
    components = uPackaging.listInstalledPackages('pui-war', 'other');
    host_ids = [ r.host_id for r in components ]
    host_ids += [1] #MN node
    return host_ids

def _reenableCustomCP(config, host_id, cp_name):
    from u import bootstrap

    request = uHCL.Request(host_id, user='root', group='root')
    jbossdir = bootstrap.getJBossDir(config.rootpath) +"/"
    src = jbossdir + "puitconf_d/" + cp_name + ".properties"
    dst = jbossdir + "puitconf/" + cp_name + ".properties"
    request.copy(src, dst)
    request.perform()

def _isSCInstalled(sc_name):
    return uPackaging.pkg_installed(1, ('sc', sc_name))


def recoverCustomCPs(config):
    known_custom_cps = { #cp_name -> sc_name
            'clm': 'CLM',
            'belgacom': 'BelgacomDNS',
            'iinetsso': 'SSO'
        }
    custom_cps_to_be_reenabled = [ cp for cp in known_custom_cps.keys() if _isSCInstalled(known_custom_cps[cp]) ]
    if custom_cps_to_be_reenabled:
        for host_id in _getPuiHosts():
            for custom_cp_name in custom_cps_to_be_reenabled:
                _reenableCustomCP(config, host_id, custom_cp_name)
        api = openapi.OpenAPI()
        api.pem.packaging.restartUIOnHosts() 

