from abc import ABCMeta, abstractmethod


def _dict_to_act_params(dict_data):
    return [{"var_name": name, "var_value": dict_data[name]} for name in dict_data]


def _act_params_to_dict(act_params):
    res = {}
    for act_param in act_params:
        res[act_param["var_name"]] = act_param["var_value"]
    return res


class _AbstractPOART(object):
    __metaclass__ = ABCMeta
    def __init__(self, rt_name, description="", act_params=None, type_id=None):
        self.reclass_name = self.get_resource_class_name()
        self.resource_type_name = rt_name
        self.description = description
        self.act_params = _dict_to_act_params(act_params) if act_params is not None else []
        self.type_id = type_id

    @abstractmethod
    def get_resource_class_name(self):
        pass

    def update_act_params(self, **kwargs):
        dict_data = _act_params_to_dict(self.act_params)
        dict_data.update(kwargs)
        self.act_params = _dict_to_act_params(dict_data)


class ServiceReferenceRT(_AbstractPOART):
    def get_resource_class_name(self):
        return "rc.saas.service.link"


class ServiceRT(_AbstractPOART):
    def get_resource_class_name(self):
        return "rc.saas.service"


class ResourceLimit(object):
    def __init__(self, resource_type, limit_value=-1):
        self.resource_type = resource_type
        self.limit_value = limit_value


def create_service_template(open_api, name, resources_and_limits):
    def load_or_create_resource_type(limit):
        if not isinstance(limit, ResourceLimit):
            raise ValueError("Expected Limit objects in resources_and_limits but got %s" % type(limit))

        resource_type = limit.resource_type
        if resource_type.type_id is None:
            resource_type.type_id = open_api.pem.addResourceType(
                resclass_name=resource_type.reclass_name,
                name=resource_type.resource_type_name,
                desc=resource_type.description,
                act_params=resource_type.act_params)["resource_type_id"]
        else:
            pass #todo better to load rt if type_id is known
        return limit

    open_api.beginRequest()
    resources_and_limits = map(load_or_create_resource_type, resources_and_limits)
    resources_oapi_structure = [{"resource_type_id": limit.resource_type.type_id} for limit in resources_and_limits]
    service_template_id = open_api.pem.addServiceTemplate(
        owner_id=1,
        name=name,
        resources= resources_oapi_structure)["st_id"]

    limits_oapi_structure = [{
                                 "resource_id": resource_limit.resource_type.type_id,
                                 "resource_limit": resource_limit.limit_value
                             } for resource_limit in resources_and_limits]
    open_api.pem.setSTRTLimits(st_id=service_template_id, limits=limits_oapi_structure)
    open_api.pem.activateST(st_id=service_template_id)
    open_api.commit()
    return {
        "resource_limits": resources_and_limits,
        "st_id": service_template_id
    }