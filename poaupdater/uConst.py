
class Const(object):

        @staticmethod
        def getWinPlatform():
                return 'win32'

        @staticmethod
        def isWinPlatform(sysPlatform):
                return sysPlatform == Const.getWinPlatform()

        @staticmethod
        def isWindows():
                import sys
                return Const.isWinPlatform(sys.platform)

        @staticmethod
        def isLinux():
                return not Const.isWindows()

        @staticmethod
        def getOsaWinPlatform():
                return 'Win32'

        @staticmethod
        def isOsaWinPlatform(osaPlatform):
                return osaPlatform == Const.getOsaWinPlatform()

        @staticmethod
        def getDistribWinDir():
                return 'win32'

        @staticmethod
        def getDistribLinDir():
                return 'RHEL'

class Modules:
    PLATFORM = 'Platform'
    APS = 'APS'
    PACI = 'PACI'
    BILLING = 'PBA'
    SHM = 'SHM'
    BCM = 'BCM'
    CSP = 'CSP'
    AZURE = 'Azure'
    SAMPLE_APPS = "Sample Applications"

    PBA = 'PBA'     # 3-N billing roles
    PBA_INTEGRATION = 'PBAIntegration'    # enabling 'Billling' button in PCP for 3-Node billing set

    CORE_MODULES = (PLATFORM, APS)
    ESSENTIALS_MODULES = (BILLING, PBA_INTEGRATION, PACI, SHM, BCM, CSP, AZURE)

ALLOW_SEND_STATISTICS_SYS_PROP = 'allow.send.statistics'
ALLOW_SEND_STATISTICS = True
