from subprocess import Popen
from poaupdater import uLogging, uPEM, uUtil, uCrypt
import os
import tempfile
import shutil
import platform


def run(c, valid_codes=[0]):
    uLogging.debug(c)
    c = c.replace("\n", " ")
    c = c.replace("\t", " ")
    return uUtil.execCommand(c, valid_codes)


def make_aps_certificates(installation):
    uLogging.debug('Making APS certificates')
    JAVA_HOME = os.getenv('JAVA_HOME', "/usr/java/default")
    KEYTOOL = os.path.join(JAVA_HOME, "bin", "keytool")
    CRED_PATH = os.path.join(installation.rootpath, "credentials")
    APS_CRED_PATH = os.path.join(installation.rootpath, "APS", "certificates")
    KEYSTORE = os.path.join(CRED_PATH, "keystore.jks")
    TRUSTSTORE = os.path.join(CRED_PATH, "truststore.jks")
    PSWD = "password"
    UUID = "5c3d720e-2307-4573-9183-5ce5837e5bd5"
    OPENSSL = uCrypt._get_openssl_binary()
    CA = os.path.join(APS_CRED_PATH, 'ca.pem')
    CTRL = os.path.join(APS_CRED_PATH, 'controller.pem')
    TMPDIR = tempfile.mkdtemp()
    SSLCNF = os.path.join(TMPDIR, 'openssl.cnf')
    CAK = os.path.join(TMPDIR, 'server.key')
    CACK = os.path.join(TMPDIR, 'server_clean.key')
    CACSR = os.path.join(TMPDIR, 'server.csr')
    CACER = os.path.join(TMPDIR, 'server.pem')
    CTRLK = os.path.join(TMPDIR, 'server2.key')
    CTRLCK = os.path.join(TMPDIR, 'server_clean2.key')
    CTRLCSR = os.path.join(TMPDIR, 'server2.csr')
    CTRLCER = os.path.join(TMPDIR, 'server2.pem')

    if not os.path.isfile(CA):
        cnf = open(SSLCNF, 'w')
        cnf.write("""
		distinguished_name  = req_distinguished_name
		[req_distinguished_name]
		[v3_req]
		[v3_ca]
		""")
        cnf.close()

        server_key = """%(OPENSSL)s genrsa -des3 -out %(CAK)s
			-passout pass:%(PSWD)s 2048""" % locals()
        csr = """%(OPENSSL)s req -new -key %(CAK)s -out %(CACSR)s
			-subj "/C=RU/O=Parallels/CN=APS CA Certificate"
			-config %(SSLCNF)s
			-passin pass:%(PSWD)s""" % locals()

        del_passphrase = """%(OPENSSL)s rsa -in %(CAK)s -out %(CACK)s
		-passin pass:%(PSWD)s""" % locals()

        cacert = """%(OPENSSL)s x509 -req -days 5475 -in %(CACSR)s -signkey %(CACK)s -out %(CACER)s
		-passin pass:%(PSWD)s""" % locals()

        server_key2 = """%(OPENSSL)s genrsa -des3 -out %(CTRLK)s
			-passout pass:%(PSWD)s 2048""" % locals()
        csr2 = """%(OPENSSL)s req -new -key %(CTRLK)s -out %(CTRLCSR)s
			-subj "/C=RU/O=Parallels/OU=Controller/CN=%(UUID)s"
			-config %(SSLCNF)s
			-passin pass:%(PSWD)s""" % locals()
        del_passphrase2 = """%(OPENSSL)s rsa -in %(CTRLK)s -out %(CTRLCK)s
		-passin pass:%(PSWD)s""" % locals()

        cntcert = """%(OPENSSL)s x509 -req -days 5475 -in %(CTRLCSR)s -signkey %(CTRLCK)s -out %(CTRLCER)s
		-passin pass:%(PSWD)s""" % locals()

        cmds = [server_key, csr, del_passphrase, cacert, server_key2, csr2, del_passphrase2, cntcert
                ]

        for i in cmds:
            run(i)

        if not os.path.exists(APS_CRED_PATH):
            os.makedirs(APS_CRED_PATH)

        merge = open(CACER).read() + open(CACK).read()
        outf = open(CA, 'w')
        outf.write(merge)
        outf.close()

        merge = open(CTRLCER).read() + open(CTRLCK).read()
        outf = open(CTRL, 'w')
        outf.write(merge)
        outf.close()

        shutil.rmtree(TMPDIR)

    if not os.path.exists(CRED_PATH):
        os.makedirs(CRED_PATH)

    keystore_check = """"%(KEYTOOL)s" -list -alias apsc  
		-keystore "%(TRUSTSTORE)s"
		-storepass %(PSWD)s
	""" % locals()
    res = run(keystore_check, valid_codes=[0, 1])
    if res == 1:
        keystore_import = """"%(KEYTOOL)s" -import -noprompt -trustcacerts -alias apsc 
			-file "%(CTRL)s"
			-keystore "%(TRUSTSTORE)s"
			-storepass %(PSWD)s
		""" % locals()
        run(keystore_import)
    uLogging.debug("APS certificates completed")

if __name__ == '__main__':
    class test:
        pass
    installation = test()
    installation.rootpath = '/usr/local/pem'
    make_aps_certificates(installation)
