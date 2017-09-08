import sys, os, socket, subprocess, getpass, pwd, logging, cStringIO, re, time
import uPgSQL, uUtil, uConfig, uSysDB, uPEM, uLogging, uLinux, uHCL, uBilling
import ConfigParser

global uUtil
PostgreSQLCmd = """ su - postgres -c "psql --port=%d -t -P format=unaligned -c \\"%s\\"" """

PG_SLAVE_END_RECOVERY_TIMEOUT_SECONDS = 600
DEFAULT_PG_PORT = 5432
PGHA_ACTIVE_CONF_PATH = "/usr/local/pem/etc/pgha.conf"

class PghaSettings:
    COMMON_SECTION_NAME = "common"

    def __init__(self, pgha_conf_path):
        self.isHa = False
        self.vip_1 = ""
        self.vip_2 = ""
        self.monitorAccount = ""
        self.monitorAccountPasswd = ""
        self.haBackendPort = 15432 # default
        self.aDbNode = ""
        self.bDbNode = ""

        if os.path.exists(pgha_conf_path):
            config = ConfigParser.ConfigParser()
            config.read(pgha_conf_path)

            self.isHa = config.has_option(PghaSettings.COMMON_SECTION_NAME, "IS_PGHA") and config.get(PghaSettings.COMMON_SECTION_NAME, "IS_PGHA").strip() == "1"
            self.vip_1 = config.get(PghaSettings.COMMON_SECTION_NAME, "PGHA_VIP").strip()
            self.vip_2 = config.get(PghaSettings.COMMON_SECTION_NAME,  "PGHA_VIP_2").strip()
            self.monitorAccount = config.get(PghaSettings.COMMON_SECTION_NAME,  "PG_MONITOR_ACCOUNT").strip()
            self.monitorAccountPasswd = config.get(PghaSettings.COMMON_SECTION_NAME, "PG_MONITOR_ACCOUNT_PASSWORD")
            self.haBackendPort = int(config.get(PghaSettings.COMMON_SECTION_NAME, "PG_BACKEND_PORT").strip())
            self.aDbNode = config.get(PghaSettings.COMMON_SECTION_NAME, "A_DB_NODE")
            self.bDbNode = config.get(PghaSettings.COMMON_SECTION_NAME, "B_DB_NODE")

def getPortPostgresIsListeningOn(run, pgPort = DEFAULT_PG_PORT):
    return run(PostgreSQLCmd % (pgPort, "SHOW port"))

def getWalKeepSegments(run, pgPort = DEFAULT_PG_PORT):
    return run(PostgreSQLCmd % (pgPort, "SELECT setting FROM pg_settings WHERE name = 'wal_keep_segments'"))

def getRecoveryConf(run, pgPort = DEFAULT_PG_PORT):
    return run(PostgreSQLCmd % (pgPort, "SELECT pg_read_file('recovery.conf')"))

def ipAddrToUserUniqPostfix(ipAddr):
    return "_"+ipAddr.replace(".", "_")

def listPgDatabases(run, pgPort = DEFAULT_PG_PORT):
    rv = run(PostgreSQLCmd % (pgPort, "SELECT datname FROM pg_database"))
    return rv.split('\n')

def checkHostPermittedToBeReplicaOfDB(run, hostName):
    authFile = "/root/auth_hosts"
    authFileBody = run(""" cat "%s" 2> /dev/null || echo -n """ % (authFile,))
    try:
        if not (hostName in authFileBody.splitlines()):
            raise Exception("Host is not authorized! Please add %s to %s file on master DB host." % (hostName, authFile))
    except IOError:
        raise Exception("Please put replicas hostnames into %s file on master DB host." % authFile)

def getPghaSettings():
    pghaSettings = PghaSettings(PGHA_ACTIVE_CONF_PATH)

    return pghaSettings

def forceHaMasterSyncConf(run):
    uLogging.info("Force Master DB node to sync PostgreSQL configuration files")
    try:
        run("""/usr/local/pem/bin/pgha/pghactl.py sync-pg-conf""")
    except Exception, ex:
        uLogging.warn("Failed to synchronise DB nodes configuration files with error '%s'" % ex.message)


def getHaMasterAddr(pghaSettings):
    run = lambda cmd: uUtil.runLocalCmd(cmd)
    cmdStatusSql = """PGPASSWORD=%s PGCONNECT_TIMEOUT=10 psql postgres -t -A --username=%s --host=%s --port=%d -c "select pg_is_in_recovery() " """

    # detect status of PG on DB node A
    try:
        aNodeRecoveryState = run(cmdStatusSql % (pghaSettings.monitorAccountPasswd, pghaSettings.monitorAccount, pghaSettings.aDbNode, pghaSettings.haBackendPort)).strip()
    except Exception, ex:
        uLogging.err("Failed to request DB node A with error '%s'" % ex.message)
        raise Exception("PostgreSQL on DB node A did not response. Both DB nodes should be operable")
    uLogging.info("DB node A recovery status '%s'" % aNodeRecoveryState)
    isAMaster = True if aNodeRecoveryState == "f" else False

    # detect status of PG on DB node B
    try:
        bNodeRecoveryState = run(cmdStatusSql % (pghaSettings.monitorAccountPasswd, pghaSettings.monitorAccount, pghaSettings.bDbNode, pghaSettings.haBackendPort)).strip()
    except Exception, ex:
        uLogging.err("Failed to request DB node B with error '%s'" % ex.message)
        raise Exception("PostgreSQL on DB node B did not response. Both DB nodes should be operable")
    uLogging.info("DB node B recovery status '%s'" % bNodeRecoveryState)
    isBMAster = True if bNodeRecoveryState == "f" else False

    # check statuses
    if isAMaster and isBMAster:
        raise Exception("Split Brain of PGHA cluster detected")

    if not isAMaster and not isBMAster:
        raise Exception("Incorrect PGHA cluster state: both DB nodes are slaves")

    return pghaSettings.aDbNode if isAMaster else pghaSettings.bDbNode # one node is Master, another is Slave, so choose Master

def iptablesConfigAllowDb(run, slaveCommunicationIP, masterPort):
    iptablesCmds = []
    iptablesCmds.append(""" iptables -D Postgres -p tcp -s %s --dport %s -j ACCEPT 2> /dev/null || echo -n """ % (slaveCommunicationIP, str(masterPort)))
    iptablesCmds.append(""" iptables -I Postgres 1 -p tcp -s %s --dport %s -j ACCEPT """ % (slaveCommunicationIP, str(masterPort)))
    iptablesCmds.append(""" service iptables save """)

    for ruleCmd in iptablesCmds:
        run(ruleCmd)

def iptablesConfigDenyDb(run, slaveCommunicationIP, masterPort):
    iptablesCmds = []
    iptablesCmds.append(""" iptables -D Postgres -p tcp -s %s --dport %s -j ACCEPT 2> /dev/null || echo -n """ % (slaveCommunicationIP, str(masterPort)))
    iptablesCmds.append(""" service iptables save """)

    for ruleCmd in iptablesCmds:
        run(ruleCmd)

class DeployPgSlaveResult(object):
    pass

def deployPgSlave(slaveHostID, isBillingMaster, masterRootPwd, readOnlyUserType, additionalIPs, slaveScript, slaveScriptArgs):
    if not uLogging.logfile:
        uLogging.init2("/var/log/pa/register_slave.log", True, False)
    uLogging.info("Deploying PostgreSQL slave server on PA service node #%d...", slaveHostID)

    masterHostID = 0
    pghaSettings = getPghaSettings()

    if not isBillingMaster:
        if slaveHostID == 1:
            raise Exception("The target slave host is MN: no possibility to use MN node as a database replica.")

        con = uSysDB.connect()
        cur = con.cursor()
        cur.execute("SELECT inet_server_addr(), inet_server_port(), current_database()")
        row = cur.fetchone()
        databaseName = row[2]
        if pghaSettings.isHa:
            masterAddr = getHaMasterAddr(pghaSettings)
            masterPort = pghaSettings.haBackendPort
            targetReplicationSourceMasterAddr = pghaSettings.vip_2
        else:
            masterAddr = row[0]
            masterPort = int(row[1])
            targetReplicationSourceMasterAddr = masterAddr
        uLogging.info("Master DB location: '%s at %d'" % (masterAddr, masterPort))

        if not (masterAddr in (x[1] for x in uLinux.listNetifaces())):
            uLogging.info("Master is automation database server running remotely at %s:%d.", masterAddr, masterPort)
            runOnMaster = uUtil.getSSHRemoteRunner(masterAddr, masterRootPwd)
            runOnMaster.isLocal = False
        else:
            uLogging.info("Master is automation database server running locally at %s:%d.", masterAddr, masterPort)
            runOnMaster = lambda cmd: uUtil.runLocalCmd(cmd)
            runOnMaster.isLocal = True
            masterHostID = 1
    else:
        billingHostID = uBilling.PBADBConf.get_host_id()
        if slaveHostID in (b.get_host_id() for b in uBilling.get_billing_hosts()):
            raise Exception("The target slave host is billing node: no possibility to use billing node as a database slave.")
        runOnMaster = lambda cmd: uHCL.runHCLCmd(billingHostID, cmd)
        runOnMaster.isLocal = False
        masterAddr = uPEM.getHostCommunicationIP(billingHostID)
        masterPort = int(getPortPostgresIsListeningOn(runOnMaster))
        targetReplicationSourceMasterAddr = masterAddr
        databaseName = "pba" # better to figureout database name dynamically
        uLogging.info("Master is billing database server running at %s:%d.", masterAddr, masterPort)
        masterHostID = billingHostID

    isPermitted = False
    slave = uPEM.getHost(slaveHostID)
    if not runOnMaster.isLocal:
        try:
            runCheck = lambda cmd: uUtil.runLocalCmd(cmd)
            checkHostPermittedToBeReplicaOfDB(runCheck, slave.name)
            isPermitted = True
        except:
            pass
    if not isPermitted:
        checkHostPermittedToBeReplicaOfDB(runOnMaster, slave.name)

    slaveCommunicationIP = uPEM.getHostCommunicationIP(slaveHostID)
    ipAddrJoined = ipAddrToUserUniqPostfix(slaveCommunicationIP)
    replUserName = "slave_oa"+ipAddrJoined
    replUserPwd = uUtil.generate_random_password(16)

    runOnSlave = lambda cmd: uHCL.runHCLCmd(slaveHostID, cmd)
    uLogging.info("Slave database server is going to be deployed at %s (%s)", slaveCommunicationIP, slave.name)
    pgsqlOnMaster = uPgSQL.PostgreSQLConfig(commander = runOnMaster)
    pgsqlVer = str(pgsqlOnMaster.get_version_as_int())

    uLogging.info("Instaling PostgreSQL Server on the slave...")
    runOnSlave("yum install -y pgtune postgresql%s postgresql%s-server postgresql%s-contrib" % (pgsqlVer, pgsqlVer, pgsqlVer))
    runOnSlave("yum reinstall -y pgtune postgresql%s postgresql%s-server postgresql%s-contrib" % (pgsqlVer, pgsqlVer, pgsqlVer))
    uLogging.info("Installation has finished!")

    uLogging.info("Initializing database on slave...")
    pgsqlOnSlave = uPgSQL.PostgreSQLConfig(commander = runOnSlave)
    pgsqlOnSlave.cleanup()
    pgsqlOnSlave.init_db()
    uLinux.configureDatabaseImpl(pgsqlOnSlave, None, [])
    uLogging.info("Saving some slave personal configuration files...")

    slavePersonalFilesBu = []
    slavePersonalFiles = (
        pgsqlOnSlave.get_data_dir()+"/server.key",
        pgsqlOnSlave.get_data_dir()+"/server.crt",
        pgsqlOnSlave.get_postgresql_conf(),
        pgsqlOnSlave.get_pghba_conf()
    )
    slavePersonalDir = os.path.dirname(pgsqlOnSlave.get_data_dir().rstrip("/"))
    for pf in slavePersonalFiles:
        runOnSlave(""" su - postgres -c 'cp -f "%s" "%s/"' """ % (pf, slavePersonalDir))
        slavePersonalFilesBu.append(os.path.join(slavePersonalDir, os.path.basename(pf)))
    pgsqlOnSlave.stop()
    uLogging.info("Database has been initialized!")

    uLogging.info("Enabling replication connection from slave to master...")
    runOnMaster(""" su - postgres -c "psql --port=%d -c \\"DROP ROLE IF EXISTS %s\\"" """ % (masterPort, replUserName,))
    runOnMaster(""" su - postgres -c "psql --port=%d -c \\"CREATE ROLE %s WITH REPLICATION ENCRYPTED PASSWORD '%s' LOGIN CONNECTION LIMIT 8\\"" """ % (masterPort, replUserName, replUserPwd))

    uLogging.info("Creating read-only user and users to be replicated from master to slave for farther readonly use on the slave node.")
    roUserName = "oa"+ipAddrJoined
    roUserPwd = uUtil.generate_random_password(32)
    runOnMaster(""" su - postgres -c "psql --port=%d --dbname=%s -c \\"REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM %s\\"" 2> /dev/null || echo -n """ % (masterPort, databaseName, roUserName))
    runOnMaster(""" su - postgres -c "psql --port=%d -c \\"DROP ROLE IF EXISTS %s\\"" """ % (masterPort, roUserName,))
    runOnMaster(""" su - postgres -c "psql --port=%d -c \\"CREATE ROLE %s WITH ENCRYPTED PASSWORD '%s' LOGIN\\"" """ % (masterPort, roUserName, roUserPwd))
    if readOnlyUserType == "uinode":
        uiBoosterTables = ("aps_resource", "aps_property_value", "aps_resource_link", "aps_application", "aps_property_info",
                           "aps_package", "aps_relation_info", "aps_relation_types", "aps_type_info", "aps_type_inheritance")
        runOnMaster(""" su - postgres -c "psql --port=%d --dbname=%s -c \\"GRANT SELECT ON TABLE %s TO %s\\"" """ % (masterPort, databaseName, ",".join(uiBoosterTables), roUserName))
    else:
        runOnMaster(""" su - postgres -c "psql --port=%d --dbname=%s -c \\"GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s\\"" """ % (masterPort, databaseName, roUserName))
    uLogging.info("Read-only user has been created.")

    ipEscpd = slaveCommunicationIP.replace(".", "\\.")
    runOnMaster(""" sed -i '/^[ \t]*hostssl[ \t]\+replication[ \t]\\+%s[ \t]\+%s\/32[ \t]\+md5[ \t]*/d' "%s" """ % (replUserName, ipEscpd, pgsqlOnMaster.get_pghba_conf()))
    runOnMaster(""" sed -i -e '$,+0a\hostssl     replication    %s     %s\/32     md5' "%s" """ % (replUserName, ipEscpd, pgsqlOnMaster.get_pghba_conf()))

    if int(getWalKeepSegments(runOnMaster, masterPort)) != 16384:
        runOnMaster(""" sed -i '/^[ \t]*wal_keep_segments[ \t]*=.*/d' "%s" """ % (pgsqlOnMaster.get_postgresql_conf(),))
        runOnMaster(""" sed -i -e '$,+0a\wal_keep_segments = 16384' "%s" """ % (pgsqlOnMaster.get_postgresql_conf(),))

    #For more details see the following KB: https://kb.odin.com/en/115916
    #Chain called Postgres could be absent if KB is not applied, so that we have to add that rules only in case if KB applied
    if runOnMaster(""" iptables -nL Postgres 2> /dev/null || echo -n """):
        uLogging.info("Configuring iptables for replication access")
        iptablesConfigAllowDb(run = runOnMaster, slaveCommunicationIP= slaveCommunicationIP, masterPort=masterPort)
        uLogging.info("Configuring iptables on master done!")

        if pghaSettings.isHa:
            pghaSlaveAddr = pghaSettings.bDbNode if masterAddr == pghaSettings.aDbNode else pghaSettings.aDbNode
            uLogging.info("Configuring iptables for replication access on PGHA slave '%s'" % pghaSlaveAddr)
            runOnPghaSlave = uUtil.getSSHRemoteRunner(pghaSlaveAddr, masterRootPwd) # providing of password is an extra measure since SSH certificates are distributed
            iptablesConfigAllowDb(run = runOnPghaSlave, slaveCommunicationIP= slaveCommunicationIP, masterPort=masterPort)
            uLogging.info("Configuring iptables o PGHA slave done!")

    pgsqlOnMaster.reload()
    uLogging.info("Replication connection has been enabled!")

    if pghaSettings.isHa:
        forceHaMasterSyncConf(runOnMaster)

    uLogging.info("Setting up initial database replication...")
    cleanPgCertificate(pgsqlOnSlave.get_data_dir(), runOnSlave) # clean certificate if exists
    baseBackupCmd = """ su - postgres -c 'PGPASSWORD=%s "%s/pg_basebackup" --xlog-method=stream --host=%s --port=%s
"--pgdata=%s" "--username=%s" --write-recovery-conf' """ % (replUserPwd, pgsqlOnSlave.get_bin_dir(), targetReplicationSourceMasterAddr, str(masterPort), pgsqlOnSlave.get_data_dir(), replUserName)
    pgsqlOnSlave.cleanup()
    #targeting errors like f.e. this-> ERROR:  could not open file "./pg_hba.conf.bak": Permission denied
    runOnMaster(""" chown -R postgres:postgres "%s" """ % (pgsqlOnMaster.get_data_dir(),))
    runOnSlave(baseBackupCmd)
    uLogging.info("Initial database replication has been done!")

    uLogging.info("Doing post-configuration...")

    dotPostgresDir = os.path.dirname(os.path.dirname(pgsqlOnSlave.get_data_dir().rstrip("/"))) + "/.postgresql"
    runOnSlave(""" su - postgres -c 'mkdir -p "%s"' """ % (dotPostgresDir,))
    runOnSlave(""" su - postgres -c 'cp -f "%s/%s" "%s/%s"' """ % (pgsqlOnSlave.get_data_dir(), "server.crt", dotPostgresDir, "root.crt"))

    for i, pf in enumerate(slavePersonalFilesBu):
        runOnSlave(""" su - postgres -c 'mv -f "%s" "%s/"' """ % (pf, os.path.dirname(slavePersonalFiles[i])))

    runOnSlave("sed -i -E 's|(.*[ \\t]+sslmode[ \\t]*=[ \\t]*)prefer([ \\t]+.*)|\\1verify-ca\\2|g' \"%s/recovery.conf\" " % (pgsqlOnSlave.get_data_dir().rstrip("/"),))
    #marking server as a hot standby
    runOnSlave(""" sed -i '/^[ \t]*hot_standby[ \t]*=.*/d' "%s" """ % (pgsqlOnSlave.get_postgresql_conf(),))
    runOnSlave(""" sed -i -e '$,+0a\\hot_standby = on' "%s" """ % (pgsqlOnSlave.get_postgresql_conf(),))
    if additionalIPs is not None:
        for ip in additionalIPs:
            ipEsc = ip.replace(".", "\\.")
            runOnSlave(""" sed -i -e '$,+0a\hostssl     all    all     %s\/32     md5' "%s" """ % (ipEsc, pgsqlOnSlave.get_pghba_conf()))
    runOnSlave(""" sed -i '/^listen_addresses/s/\*/127.0.0.1/g' "%s" """ % (pgsqlOnSlave.get_postgresql_conf(),))
    uLogging.info("Post-configuration has been done!")

    uLogging.info("Starting new slave database server!")
    pgsqlOnSlave.restart()
    pgsqlOnSlave.set_autostart()
    waitSlaveRecoveryComplete(runOnSlave) # make sure recovery stage is complete

    uLinux.tunePostgresLogs(runOnSlave)
    uLogging.info("New slave database server has started!")

    if slaveScript:
        uLogging.info("Running post configuration script on slave: %s", slaveScript)
        cbCmd = """python "%s" connect_slave "%s" "%s" "%s" "%s" """ % (slaveScript, slaveCommunicationIP, databaseName, roUserName, roUserPwd)
        for a in slaveScriptArgs:
            cbCmd = cbCmd + ' "%s" ' % a
        runOnSlave(cbCmd)
        uLogging.info("Post configuration has been done!")

    rv = DeployPgSlaveResult()
    rv.replUserName = replUserName
    rv.roUserName = roUserName
    rv.masterHostID = masterHostID
    rv.masterAddr = masterAddr
    return rv

def cleanPgCertificate(pgDataPath, run):
    dotPostgresDir = os.path.dirname(os.path.dirname(pgDataPath.rstrip("/"))) + "/.postgresql"
    crtFilePath = os.path.join(dotPostgresDir, "root.crt")
    run("""rm -f "%s" 2> /dev/null""" % crtFilePath)

def waitSlaveRecoveryComplete(run):
    recoveryComplete = False
    waitRecoveryEnd = 0
    while waitRecoveryEnd < PG_SLAVE_END_RECOVERY_TIMEOUT_SECONDS:
        try:
            run("""su - postgres -c "psql -t -P format=unaligned -c $'show server_version'" """ )
            recoveryComplete = True
            break
        except Exception, e:
            errorMessage = e.message
            if errorMessage.find("system is starting") != -1:
                uLogging.warn("Slave database is in recovery mode:\n%s\n'%s'\n%s " % ("="*40 , errorMessage , "="*40))
            else:
                raise Exception("Failed to start slave database server with error '%s'!" % errorMessage)

        time.sleep(5)
        waitRecoveryEnd += 5
        uLogging.info("Wait until PG is out of the recovery stage. Elapsed '%d' seconds" % waitRecoveryEnd)

    if not recoveryComplete:
        raise Exception("Slave PostgreSQL did not complete recovery stage in '%d' seconds" % PG_SLAVE_END_RECOVERY_TIMEOUT_SECONDS)

def removePgSlave(slaveHostID, masterRootPwd):
    if not uLogging.logfile:
        uLogging.init2("/var/log/pa/deregister_slave.log", True, False)

    slave = uPEM.getHost(slaveHostID)
    slaveCommunicationIP = uPEM.getHostCommunicationIP(slaveHostID)
    runOnSlave = lambda cmd: uHCL.runHCLCmd(slaveHostID, cmd)
    uLogging.info("Slave database server at %s (%s) is going to be removed.", slaveCommunicationIP, slave.name)

    pghaSettings = getPghaSettings()

    conInfoDict = {}
    recoveryConf = getRecoveryConf(runOnSlave)
    conInfoRe = re.compile("^[ \t]*primary_conninfo[ \t]*=[ \t]*'(.*)'$")
    for l in recoveryConf.split('\n'):
        conMatch = conInfoRe.match(l)
        if conMatch:
            for kv in conMatch.group(1).split(" "):
                kv = kv.strip()
                if kv:
                    k, v = kv.split("=", 1)
                    conInfoDict[k.strip()] = v.strip()
            break

    runOnMaster = None

    recoveryHost = conInfoDict["host"]

    if pghaSettings.isHa:
        masterAddr = getHaMasterAddr(pghaSettings)
        masterPort = pghaSettings.haBackendPort
    else:
        masterAddr = recoveryHost
        masterPort = int(conInfoDict["port"])

    if recoveryHost != uPEM.getHostCommunicationIP(1): #MN node?
        billingHostID = None
        try:
            billingHostID = uBilling.PBADBConf.get_host_id()
        except Exception as e:
            uLogging.info("%s", str(e))
            pass
        if (billingHostID is not None) and (masterAddr == uPEM.getHostCommunicationIP(billingHostID)): #Billing DB?
            uLogging.info("Master is billing database server running at %s:%d.", masterAddr, masterPort)
            runOnMaster = lambda cmd: uHCL.runHCLCmd(billingHostID, cmd)
    else:
        uLogging.info("Master is automation database server running locally at %s:%d.", masterAddr, masterPort)
        runOnMaster = lambda cmd: uUtil.runLocalCmd(cmd)
    if runOnMaster is None: #Master is running as an external database server
        uLogging.info("Master is automation database server running remotely at %s:%d.", masterAddr, masterPort)
        runOnMaster = uUtil.getSSHRemoteRunner(masterAddr, masterRootPwd)

    pgsqlOnMaster = uPgSQL.PostgreSQLConfig(commander = runOnMaster)
    replUserName = conInfoDict["user"]
    uLogging.info("Disabling replication connection from slave to master...")
    ipEscpd = slaveCommunicationIP.replace(".", "\\.")
    runOnMaster(""" sed -i '/^[ \t]*hostssl[ \t]\+replication[ \t]\\+%s[ \t]\+%s\/32[ \t]\+md5[ \t]*/d' "%s" """ % (replUserName, ipEscpd, pgsqlOnMaster.get_pghba_conf()))
    runOnMaster(""" su - postgres -c "psql --port=%d -c \\"DROP ROLE IF EXISTS %s\\"" """ % (masterPort, replUserName,))

    if pghaSettings.isHa:
        forceHaMasterSyncConf(runOnMaster)

    uLogging.info("Dropping slave read only user...")
    roUserName = "oa"+ipAddrToUserUniqPostfix(slaveCommunicationIP)
    for db in listPgDatabases(runOnMaster, masterPort):
        db = db.strip()
        if not (db in ("postgres", "template0", "template1")):
            runOnMaster(""" su - postgres -c "psql --port=%d --dbname=%s -c \\"REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM %s\\"" 2> /dev/null || echo -n """ % (masterPort, db, roUserName))
    runOnMaster(""" su - postgres -c "psql --port=%d -c \\"DROP ROLE IF EXISTS %s\\"" """ % (masterPort, roUserName,))

    if runOnMaster(""" iptables -nL Postgres 2> /dev/null || echo -n """):
        uLogging.info("Dropping iptables rules...")
        iptablesConfigDenyDb(run = runOnMaster, slaveCommunicationIP = slaveCommunicationIP, masterPort=masterPort)
        uLogging.info("Iptables rules on master are dropped!")

        if pghaSettings.isHa:
            pghaSlaveAddr = pghaSettings.bDbNode if masterAddr == pghaSettings.aDbNode else pghaSettings.aDbNode
            runOnPghaSlave = uUtil.getSSHRemoteRunner(pghaSlaveAddr, masterRootPwd) # providing of password is an extra measure since SSH certificates are distributed
            uLogging.info("Dropping PGHA slave iptables rules...")
            iptablesConfigDenyDb(run = runOnPghaSlave, slaveCommunicationIP = slaveCommunicationIP, masterPort=masterPort)
            uLogging.info("Iptables rules on PGHA slave are dropped!")

    uLogging.info("Stopping slave server...")
    pgsqlOnSlave = uPgSQL.PostgreSQLConfig(commander = runOnSlave)
    pgsqlOnSlave.stop()
    uLogging.info("Reloading master configuration...")
    pgsqlOnMaster.reload()
    uLogging.info("Cleanup slave server data...")
    pgsqlOnSlave.cleanup()
    cleanPgCertificate(pgsqlOnSlave.get_data_dir(), runOnSlave) # delete certificate
    uLogging.info("Removing slave has finished!")

__all__ = ["deployPgSlave", "removePgSlave"]
