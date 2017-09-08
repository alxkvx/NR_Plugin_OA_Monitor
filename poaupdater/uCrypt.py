import string
import random
import base64
import os
from subprocess import Popen, PIPE

import uPEM
import uUtil
import uLogging

from uConst import Const


def decryptAESCrypto(data, key):
    from Crypto.Cipher import AES
    iv, ctext = [base64.decodestring(x) for x in data.split('$')[-2:]]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    # pycrypto adds \x0f if data str < 32 symbols
    return filter(lambda x: x in string.printable, cipher.decrypt(ctext))

# Try to decrypt AES via ssl, if we do not have installed python-crypto


def _hexstr(data):
    dig2 = lambda x: ('%2s' % x).replace(' ', '0')
    str = ''
    for c in map(lambda x: dig2(hex(ord(x))[2:]), data):
        str += c
    return str


def decryptAESOpenSSL(data, key):
    cbc, iv_base64, ctext_base64 = [x for x in data.split('$')[-3:]]
    iv_hex = _hexstr(base64.decodestring(iv_base64))
    key_hex = _hexstr(key)

    cmd = "%s enc -%s -d -a -K %s -iv %s" % (_get_openssl_binary(), cbc, key_hex, iv_hex)
    p = Popen(cmd, shell=True,
              stdin=PIPE, stdout=PIPE, stderr=PIPE)
    p.stdin.write(ctext_base64 + '\n')
    (out, err) = p.communicate()
    if err:
        raise Exception('Error while decrypt AES: %s' % err)
    return out


def decryptData(data):
    if not data.startswith("$AES"):
        return data

    key = base64.decodestring(uPEM.getPleskdProps()['encryption_key'])
    try:
        from Crypto.Cipher import AES
    except ImportError:
        return decryptAESOpenSSL(data, key)

    return decryptAESCrypto(data, key)


def _pad(bs, s):
    return s + (bs - len(s) % bs) * chr(bs - len(s) % bs)


def generateIvHex(bs):
    if hasattr(random, 'SystemRandom'):
        choices = string.ascii_uppercase + string.ascii_lowercase + string.digits
        iv = ''.join(random.SystemRandom().choice(choices) for _ in xrange(bs))
        return _hexstr(iv)
    else:
        devrnd = file('/dev/urandom', 'r')
        iv = devrnd.read(bs)
        devrnd.close()
        return iv


def encryptAESOpenSSL(data, key):
    block_size = 16
    iv_hex = generateIvHex(block_size)
    key_hex = _hexstr(key)

    cmd = "%s enc -aes-128-cbc -a -nopad -K %s -iv %s" % (_get_openssl_binary(), key_hex, iv_hex)
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    p.stdin.write(_pad(block_size, data))
    (out, err) = p.communicate()
    if err:
        raise Exception('Error while decrypt AES: %s' % err)
    return "$AES-128-CBC${0}${1}".format(base64.b64encode(iv_hex.decode("hex")), out[:-1])


def encryptAESCrypto(data, key):
    from Crypto.Cipher import AES

    block_size = AES.block_size

    iv = generateIvHex(block_size).decode('hex')
    cipher = AES.new(key, AES.MODE_CBC, iv)

    return "$AES-128-CBC${0}${1}".format(base64.b64encode(iv), base64.b64encode(cipher.encrypt(_pad(block_size, data))))


def encryptData(data, b64key=None):
    if data.startswith("$AES"):
        return data

    if b64key is None:
        b64key = uPEM.getPleskdProps()['encryption_key']

    key = base64.decodestring(b64key)
    try:
        from Crypto.Cipher import AES
    except ImportError:
        return encryptAESOpenSSL(data, key)

    return encryptAESCrypto(data, key)


def _get_openssl_binary():
    if Const.isWindows():
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "openssl.exe")
    else:
        return "openssl"


def generate_rsa_key():
    env = os.environ.copy()
    openssl_binary = _get_openssl_binary()
    stdout, stderr, status = uUtil.readCmdExt([openssl_binary, 'genrsa', '1024'], env=env)

    pempriv = stdout
    b64der = ''.join([i for i in pempriv.split('\n') if '----' not in i])

    stdout, stderr, status = uUtil.readCmdExt([openssl_binary, 'rsa', '-pubout'], input_data=pempriv, env=env)

    pempub = stdout
    b64derpub = ''.join([i for i in pempub.split('\n') if '----' not in i])
    return b64der, b64derpub


def generate_self_signed_certificate(subj):
    openssl_conf = """
    #
    # OpenSSL configuration file.
    #
     
    # Establish working directory.
     
    dir                 = .
     
    [ ca ]
    default_ca              = CA_default
     
    [ CA_default ]
    serial                  = $dir/serial
    database                = $dir/certindex.txt
    new_certs_dir               = $dir/certs
    certificate             = $dir/cacert.pem
    private_key             = $dir/private/cakey.pem
    default_days                = 36500
    default_md              = md5
    preserve                = no
    email_in_dn             = no
    nameopt                 = default_ca
    certopt                 = default_ca
    policy                  = policy_match
     
    [ policy_match ]
    countryName             = match
    stateOrProvinceName         = match
    organizationName            = match
    organizationalUnitName          = optional
    commonName              = supplied
    emailAddress                = optional
     
    [ req ]
    default_bits                = 2048          # Size of keys
    default_keyfile             = key.pem       # name of generated keys
    default_md              = md5               # message digest algorithm
    string_mask             = nombstr       # permitted characters
    distinguished_name          = req_distinguished_name
    req_extensions              = v3_req
     
    [ req_distinguished_name ]
    # Variable name             Prompt string
    #-------------------------    ----------------------------------
    0.organizationName          = Organization Name (company)
    organizationalUnitName          = Organizational Unit Name (department, division)
    emailAddress                = Email Address
    emailAddress_max            = 40
    localityName                = Locality Name (city, district)
    stateOrProvinceName         = State or Province Name (full name)
    countryName             = Country Name (2 letter code)
    countryName_min             = 2
    countryName_max             = 2
    commonName              = Common Name (hostname, IP, or your name)
    commonName_max              = 64

    [ v3_ca ]
    basicConstraints            = CA:TRUE
    subjectKeyIdentifier            = hash
    authorityKeyIdentifier          = keyid:always,issuer:always
     
    [ v3_req ]
    basicConstraints            = CA:FALSE
    subjectKeyIdentifier            = hash
    """

    platform, root = uPEM.getMNInfo()

    privkey_path = os.path.join(root, 'priv_key.pem')
    cert_path = os.path.join(root, 'cert.pem')
    ssl_conf_path = os.path.join(root, 'pem_openssl.cnf')

    uLogging.debug("creating SSL config file at '%s'" % ssl_conf_path)

    ssl_cnf_file = open(ssl_conf_path, 'w+')
    ssl_cnf_file.write(openssl_conf)
    ssl_cnf_file.close()

    openssl_binary = _get_openssl_binary()
    out_text, err_text, status = uUtil.readCmdExt([openssl_binary, "req", "-new", "-x509", "-newkey", "rsa:2048", "-keyout",
                                                   privkey_path, "-out", cert_path, "-days", "36500", "-subj", subj, "-nodes", "-config", ssl_conf_path], env=os.environ.copy())

    uLogging.debug("openssl exited with status: %s", status)
    uLogging.debug("openssl executed with result:\nstderr:\n%s\nstdout:\n%s\n", err_text, out_text)

    # Read created private key
    privkey_file = open(privkey_path, 'r')
    b64privkey = privkey_file.read()
    privkey_file.close()

    # Read created certificate
    cert_file = open(cert_path, 'r')
    b64cert = cert_file.read()
    cert_file.close()

    # cleanup
    os.remove(privkey_path)
    os.remove(cert_path)
    os.remove(ssl_conf_path)

    return b64privkey, b64cert


def generateEncryptionKey():
    import struct

    if hasattr(random, 'SystemRandom'):
        sysrnd = random.SystemRandom()
        key = [struct.pack('B', sysrnd.getrandbits(8)) for _ in xrange(16)]
        key = "".join(key)
    else:
        devrnd = file('/dev/urandom', 'r')
        key = devrnd.read(16)
        devrnd.close()

    b64key = base64.encodestring(key)
    return b64key
