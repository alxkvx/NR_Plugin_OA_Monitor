__author__ = 'imartynov'

import sys
from uConst import Const

from poaupdater import uUtil, uLogging
if not Const.isWindows():
    from poaupdater import uPgSQL


def tuneDatabase(config):
    uLogging.debug("tuneDatabase started, scale_down: %s" % config.scale_down)
    if not Const.isWindows() and config.scale_down:
        uLogging.debug("tuning PgSQL")
        p = uPgSQL.PostgreSQLConfig()
        uUtil.replaceInFile(p.get_postgresql_conf(), 'max_connections =.*',
                            'max_connections = 128 # tune for scale down', True)
        uUtil.replaceInFile(p.get_postgresql_conf(), 'shared_buffers =.*',
                            'shared_buffers = 512MB # tune for scale down', True)

        uLogging.debug("restarting PgSQL...")
        p.restart()
    else:
        uLogging.debug("nothing done")


def tuneJBoss(config):
    uLogging.debug("tuneJBoss started, scale_down: %s" % config.scale_down)
    if not Const.isWindows() and config.scale_down:
        from u import bootstrap
        jbossdir = bootstrap.getJBossDir(config.rootpath)

        uLogging.info("Tuning pau service JVM")
        uUtil.replaceInFile(jbossdir + '/bin/standalone.conf', '-Xmx2048m', '-Xmx1024m -Xss256k')

        uLogging.info("Tuning JBoss connection pool")
        bootstrap.execCLI(
            jbossdir, '-c', '/subsystem=datasources/data-source=pauds:write-attribute(name="max-pool-size", value="80")')
        # jboss restart required, performed after PUI deployment
    else:
        uLogging.debug("nothing done")
