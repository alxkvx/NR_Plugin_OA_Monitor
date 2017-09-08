import uBilling
import uLogging
import uPackaging
from uPoaServiceTemplateCreator import ResourceLimit
from uPoaServiceTemplateCreator import create_service_template


class Resource(ResourceLimit):
    def __init__(self, resource_type, limit_value, resource_rate=None):
        ResourceLimit.__init__(self, resource_type=resource_type, limit_value=limit_value)
        self.resource_rate = resource_rate


class ResourceRate(object):
    def __init__(self, resource_category, setup_fee=0.0, recurring_fee=0.0, overusage_fee=0.0, show_in_store=False,
                 included=0.0, maximum=-1.0):
        self.resource_category = resource_category
        self.setup_fee = setup_fee
        self.recurring_fee = recurring_fee
        self.overusage_fee = overusage_fee
        self.show_in_store = show_in_store
        self.included = included
        self.maximum = maximum


class PlanPeriod(object):
    def __init__(self, period, period_type, setup_fee, recurring_fee, renewal_fee, deposit_fee, active=True,
                 trial=False, transfer_fee=None, refund_period=None, prorated_refund_after_refund_period=False,
                 sort_number=None):
        self.period = period
        self.period_type = period_type
        self.setup_fee = setup_fee
        self.recurring_fee = recurring_fee
        self.renewal_fee = renewal_fee
        self.deposit_fee = deposit_fee
        self.active = active
        self.trial = trial
        self.transfer_fee = transfer_fee
        self.refund_period = refund_period
        self.prorated_refund_after_refund_period = prorated_refund_after_refund_period
        self.sort_number = sort_number


def create_sales_category(open_api, sales_category_name):
    store_screen_id = sales_category_name.upper().replace(' ', '_') + '_SCREEN'
    sales_categories = [{'name': sales_category_name, 'shortDescription': sales_category_name}]
    try:
        open_api.bss.addStoreScreen(id=store_screen_id, storeTemplateId='HOSTING', salesCategories=sales_categories)
        uLogging.debug("'%s' online store screen has been created" % store_screen_id)
    except Exception, e:
        if 'Table Store Screen already contains' in str(e):
            uLogging.debug("Store Screen '%s' is already exists." % store_screen_id)
        else:
            raise e


def create_service_plan(open_api, sp_name, sales_category_name, plan_periods, st):
    _plan_periods = []
    for plan_period in plan_periods:
        _plan_periods.append({
            'period': plan_period.period,
            'periodType': plan_period.period_type,
            'setupFee': plan_period.setup_fee,
            'recurringFee': plan_period.recurring_fee,
            'renewalFee': plan_period.renewal_fee,
            'depositFee': plan_period.deposit_fee,
            'active': plan_period.active,
            'trial': plan_period.trial,
            'transferFee': plan_period.transfer_fee,
            'refundPeriod': plan_period.refund_period,
            'proratedRefundAfterRefundPeriod': plan_period.prorated_refund_after_refund_period,
            'sortNumber': plan_period.sort_number
        })

    resource_rates = []
    for resource in st['resource_limits']:
        if isinstance(resource, Resource) and resource.resource_rate is not None:
            resource_rates.append({
                'resourceId': resource.resource_type.type_id,
                'resourceCategory': resource.resource_rate.resource_category,
                'setupFee': resource.resource_rate.setup_fee,
                'recurringFee': resource.resource_rate.recurring_fee,
                'overusageFee': resource.resource_rate.overusage_fee,
                'showInStore': resource.resource_rate.show_in_store,
                'included': resource.resource_rate.included,
                'maximum': resource.resource_rate.maximum
            })
    open_api.bss.addServicePlan(name=sp_name,
                                serviceTemplateId=st['st_id'],
                                shortDescription=sp_name,
                                longDescription=sp_name,
                                planPeriods=_plan_periods,
                                resourceRates=resource_rates,
                                salesCategories=[sales_category_name])
    uLogging.debug("Service plan '%s' has been added" % sp_name)


def create_service_template_and_service_plan_with_sales_category(open_api, st_name, sp_name,
                                                                 sales_category_name,
                                                                 plan_periods,
                                                                 resources):
    st = create_service_template(open_api, name=st_name, resources_and_limits=resources)
    if len(uPackaging.listInstalledPackages('PPAB', 'other')) == 1 or (len(uBilling.get_billing_hosts()) > 0):
        create_sales_category(open_api, sales_category_name=sales_category_name)
        create_service_plan(open_api, sp_name=sp_name, sales_category_name=sales_category_name,
                            plan_periods=plan_periods, st=st)
