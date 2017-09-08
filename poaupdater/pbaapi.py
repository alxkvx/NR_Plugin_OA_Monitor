#!/usr/bin/python

import base64
import xmlrpclib as xmlrpc

import uLogging
import uSysDB


class PBAAPISettings:

    def __init__(self):
        self.host = None
        self.port = 5224
        self.uri = "/RPC2"
        self.ssl = False
        self.username = None
        self.password = None

    def url(self):
        if (self.ssl):
            return "https://%s:%s/%s" % (self.host, self.port, self.uri)
        else:
            return "http://%s:%s/%s" % (self.host, self.port, self.uri)

_settings = PBAAPISettings()


def init(host, port, uri, ssl=False, username=None, password=None):
    global _settings
    _settings.host = host
    _settings.port = port
    _settings.uri = uri
    _settings.ssl = ssl
    _settings.username = username
    _settings.password = password


def initFromEnv():
    import uPEM
    import uCrypt
    con = uSysDB.connect()

    bmBridgeExists = uSysDB.table_exist(con, "bmbridge_settings")
    if not bmBridgeExists:
        raise Exception(
            "bmbridge_settings table doesn't exist in Operation Automation database, Operation Automation is not properly integrated with Billing.")

    cur = con.cursor()
    cur.execute(
        "SELECT name, value FROM bmbridge_settings WHERE name in ('bm.xmlrpc.host', 'bm.xmlrpc.port', 'bm.xmlrpc.uri', 'bm.xmlrpc.ssl', 'bm.xmlrpc.user', 'bm.common.password')")
    rows = cur.fetchall()

    options = {}
    for row in rows:
        options[row[0]] = row[1]

    global _settings
    _settings.host = options['bm.xmlrpc.host']
    _settings.port = options['bm.xmlrpc.port']
    _settings.uri = options['bm.xmlrpc.uri']
    _settings.ssl = options.get('bm.xmlrpc.ssl') == "1"
    _settings.username = options.get('bm.xmlrpc.user')
    if _settings.username:
        _settings.password = uCrypt.decryptData(options.get('bm.common.password'))

class PBAAPIRaw(object):

    class Server(object):
        def __init__(self, name, api):
            self.name = name
            self.api = api
        def __getattr__(self, name):
            return PBAAPIRaw.Method(name, self)

    class Method(object):
        def __init__(self, name, server):
            self.name = name
            self.server = server
        def __call__(self, *args):
            return self.server.api(self.server.name, self.name, *args)


    def __init__(self, autoCommit = True):
        self.server = xmlrpc.ServerProxy(_settings.url())
        self.autoCommit = autoCommit
        self.txn_id = None

    def __getattr__(self, name):
        return PBAAPIRaw.Server(name, self)

    @staticmethod
    def hide_sensitive_data(method, args):
        import copy
        import re
        modified_args = copy.deepcopy(args)
        pass_pattern = re.compile(".*(SetAuthentication).*")
        if pass_pattern.match(method):
            modified_args = "***"
        return method, modified_args

    def __call__(self, server, method, *args):
        try:
            request = {'Server' : server, 'Method': method, 'Params': args}

            if _settings.username:
                request["Username"] = _settings.username
            if _settings.username and _settings.password:
                request["Password"] = _settings.password

            if not self.autoCommit:
                request["AutoCommit"] = "No"
            if self.txn_id is not None:
                request['TransactionID'] = self.txn_id
            uLogging.debug('Billing API call {0}::{1} {2}'.format(server, *self.hide_sensitive_data(method, args)))

            response = self.server.Execute(request)

            uLogging.debug('Billing API response: {0}'.format(response))
            if 'TransactionID' in response:
                self.txn_id = response['TransactionID']

            return response
        except xmlrpc.Fault, e:
            uLogging.debug(str(e))
            try:
                e.faultString = base64.decodestring(e.faultString)
            except:
                pass
            uLogging.debug('Billing API call: {0}::{2} {3} raised exception: {1}'
                           .format(server, e.faultString, *self.hide_sensitive_data(method, args)))
            raise
        except Exception, e:
            uLogging.debug('Billing API call: {0}::{1} {2} raised exception: '
                           .format(server, *self.hide_sensitive_data(method, args)) + str(e))
            raise


    def commit(self, raiseErrorIfTransactionNotOpened=False):
        self._close_tran('CommitTransaction', raiseErrorIfTransactionNotOpened)

    def rollback(self, raiseErrorIfTransactionNotOpened=False):
        self._close_tran('RollbackTransaction', raiseErrorIfTransactionNotOpened)

    def _close_tran(self, method, raiseErrorIfTransactionNotOpened):
        requestData = { }

        if self.txn_id is not None:
            requestData['TransactionID'] = self.txn_id
        else:
            if raiseErrorIfTransactionNotOpened:
                raise TransactionNotFound()
            else:
                return

        try:
            uLogging.debug("call method: %s(%s)" % (method, requestData))
            resp = getattr(self.server, method)(requestData)
            uLogging.debug("return %s" % (resp))
            self.txn_id = None
        except xmlrpc.Fault, e:
            uLogging.debug(str(e))
            try:
                e.faultString = base64.decodestring(e.faultString)
            except:
                pass
            uLogging.debug("Billing typed call: %s raised exception: %s" % (method, e.faultString))
            raise
        except Exception, e:
            uLogging.debug("Billing typed call: %s raised exception" % (method) + str(e))
            raise



# Typed wrappers around PBA api it doesn't require names in requests and doesn't provide
# them in responses


class SubscriptionDetails:

    def __init__(self, response):
        self.SubscriptionName = response[1]
        self.AccountID = response[2]
        self.PlanID = response[3]
        self.PlanName = response[4]
        self.Status = response[5]
        self.ServStatus = response[6]

        self.source = response


class PlanDetails:

    def __init__(self, response):
        self.PlanID = response[0]
        self.Name = response[1]
        self.CategoryID = response[2]
        self.ResourceCurrencyID = response[3]
        self.ShortDescription = response[4]
        self.LongDescription = response[5]
        self.GateName = response[6]
        self.GroupID = response[7]
        self.IsParentReq = response[8]
        self.RecurringType = response[9]
        self.BillingPeriodType = response[10]
        self.BillingPeriod = response[11]
        self.ShowPriority = response[12]
        self.Default_PlanPeriodID = response[13]
        self.IsOTFI = response[14]
        self.DocID = response[15]

        self.source = response


class Plan:

    def __init__(self, response):
        self.PlanID = response[0]
        self.Name = response[1]
        self.ServiceTemplateID = response[2]
        self.PlanCategoryID = response[3]
        self.ServiceTermID = response[4]
        self.ShortDescription = response[5]
        self.LongDescription = response[6]
        self.GroupID = response[7]
        self.Published = response[8]
        self.AttachUsageStatistics = response[9]
        self.AccountID = response[10]
        self.NotificationTemplate = response[11]
        self.ClassID = response[12]

        responseLength = len(response)
        if (responseLength > 24):
            baseIndex = 0
            self.RecurringType = response[baseIndex + 13]
            self.BillingPeriodType = response[baseIndex + 14]
            self.BillingPeriod = response[baseIndex + 15]
            self.FixedBillingPeriodDescr = response[baseIndex + 16]
            if responseLength > 26:
                self.PricePeriodType = response[17]  # "PricePeriodType" was added in 6.1
                baseIndex = 1

            self.AutoRenewDescription = response[baseIndex + 17]
            self.AutoRenewPlanPeriodID = response[baseIndex + 18]
            self.ShowPriority = response[baseIndex + 19]
            self.DefaultPlanPeriodID = response[baseIndex + 20]
            self.ScheduleID = response[baseIndex + 21]
            self.IsOTFI = response[baseIndex + 22]
            self.Image = response[baseIndex + 23]
            self.SwitcherShow = response[baseIndex + 24]

        if (responseLength == 24):
            self.BillingPeriodType = response[13]
            self.BillingPeriod = response[14]
            self.RecurringType = response[15]
            self.AutoRenewDescription = response[16]
            self.AutoRenewPlanPeriodID = response[17]
            self.ShowPriority = response[18]
            self.DefaultPlanPeriodID = response[19]
            self.ScheduleID = response[20]
            self.IsOTFI = response[21]
            self.Image = response[22]
            self.SwitcherShow = response[23]

        self.AutoRenew = 0
        self.AutoRenewInterval = ''
        if self.AutoRenewDescription == "On Last Statement Day":
            self.AutoRenew = 5
        if self.AutoRenewDescription.endswith(" before Expiration Date"):
            self.AutoRenew = 15
            self.AutoRenewInterval = int(self.AutoRenewDescription.split()[0])

        # etc ...

        self.source = response


class Subscription:

    def __init__(self, response):
        self.SubscriptionID = response[0]
        self.Name = response[1],
        self.AccountID = response[2],
        self.PlanID = response[3],
        self.SubscriptionPeriod = response[7]
        # etc ...

        self.source = response


class Order:

    def __init__(self, response):
        self.OrderID = response[0]
        # etc ...

        self.source = response


class PlanPeriod:

    def __init__(self, response):
        self.PlanPeriodID = response[0]
        self.Period = response[1]
        self.PeriodType = response[2]
        self.Trial = response[3]
        self.SetupFee = response[4]
        self.RecurringFee = response[5]
        self.RenewalFee = response[6]
        self.TransferFee = response[7]
        self.NonRefundableAmt = response[8]
        self.RefundPeriod = response[9]
        self.Enabled = response[10]
        self.NumberOfPeriods = response[11]
        self.FeeText = response[12]
        self.SortNumber = response[13]
        self.IsOTFI = response[14]
        self.DepositFee = response[15]
        self.DepositDescr = response[16]
        # etc ...

        self.source = response


class ResourceMapping:

    def __init__(self, response):
        self.FromID = response[0]
        self.ToID = response[4]
        # etc ...

        self.source = response


class Version:

    def __init__(self, response):
        self.Version = response[0]
        # etc ...

        self.source = response


class ServiceTemplate:

    def __init__(self, response):
        self.Id = response[0]
        self.Name = response[1]
        self.DestroyOnCancel = response[7]
        self.DomainType = response[8]
        # etc ...

        self.source = response


class ResourceCategory:

    def __init__(self, response):
        self.Id = response[0]
        self.Name = response[1]
        # etc ...
        self.source = response


class ResultWithStatus:

    def __init__(self, response):
        self.Status = response["Status"]
        self.source = response


class ResultWithIDInStatus(ResultWithStatus):

    def __init__(self, response):
        ResultWithStatus.__init__(self, response)
        self.ID = int(self.Status.split("#")[1].split()[0])


class ResultWithID:

    def __init__(self, response):
        self.ID = response[0]
        self.source = response


class ResultWithNameAndID(ResultWithID):

    def __init__(self, response):
        ResultWithID.__init__(self, response)
        self.Name = response[1]


class Void:

    def __init__(self, response):
        self.source = response


class _TypedMethod:
    resultTypes = {
        "AddPlansToStoreCategory": ResultWithStatus,
        "AddResourceToCategory": Void,
        "GetMyResourceCategories": ResourceCategory,
        "GetPlansByCategory": ResultWithNameAndID,
        "GetResourceMapping": ResourceMapping,
        "GetSalesCategoriesReportList": ResultWithID,
        "GetVersion_API": Version,
        "PlacePlanPeriodSwitchOrder_API": Order,
        "PlanAdd": ResultWithID,
        "PlanChangeAdd": ResultWithStatus,
        "PlanGet": Plan,
        "PlanPeriodListGet_API": PlanPeriod,
        "PlanRateAdd": ResultWithStatus,
        "PlanUpdate": ResultWithIDInStatus,
        "ResourceCategoryAdd": Void,
        "ServiceTemplateGet": ServiceTemplate,
        "ServiceTemplateUpdateInternal": Void,
        "SubscriptionDetailsGet_API": SubscriptionDetails,
        "SubscriptionGet": Subscription
    }

    def __init__(self, api, name):
        self.api = api
        self.name = name

    def __call__(self, *args):
        server = 'BM'
        if self.name == "ServiceTemplateUpdateInternal":
            server = 'PEMGATE'

        response = self.api(server, self.name, *args)
        results = response["Result"][0]

        if self.name in self.resultTypes:
            ResultType = self.resultTypes[self.name]
            if list == type(results) and len(results) > 0 and list == type(results[0]):
                return [ResultType(result) for result in results]
            elif len(results) > 0:
                return ResultType(results)
            else:
                return []
        else:
            return results


class PBAAPI(PBAAPIRaw):

    def __init__(self, autoCommit=True):
        PBAAPIRaw.__init__(self, autoCommit)	

    def __getattr__(self, name):
        return _TypedMethod(self, name)

