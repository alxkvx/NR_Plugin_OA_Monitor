import xmlrpclib
import openapi


EDIT_BY_PROVIDER = "p"
EDIT_BY_RESELLER = "r"
EDIT_BY_NOBODY = "n"


class BoolPropType:

    """PEM 2.7 compatibility class"""
    pass


def _registerBoolPropertyInPEM27(name, is_visible, defval):
    """This function is called for PEM 2.7 only."""
    import uSysDB
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO confman_parameters (name, type, is_visible, user_id) VALUES (%s, %s, %s, %s)",
        (name, 0, int(is_visible), 1))
    prop_id = uSysDB.get_last_inserted_value(con, "confman_parameters")
    cur.execute(
        "INSERT INTO confman_bool_parameters (parameter_id, value, vlimit) VALUES (%s, %s, %s)",
        (prop_id, int(defval), -1))


def _prepareAPI():
    return openapi.OpenAPI()


class BooleanPropType:

    def __init__(self, default_value):
        self.defval = default_value

    def update(self, param):
        param.update({'bool_prop': {'default_value': xmlrpclib.Boolean(self.defval)}})


class BooleanPropValue:

    def __init__(self, value):
        self.value = value

    def update(self, param):
        param.update({'bool_value': xmlrpclib.Boolean(self.value)})


class StringPropType:

    def __init__(self, default_value, value=None):
        self.defval = default_value
        if not value:
            self.value = default_value
        else:
            self.value = value

    def update(self, param):
        param.update({'str_prop': {'default_value': self.defval}})
        param.update({'str_value': self.value})


class NumPropType:

    def __init__(self, default_value, min_limit, max_limit):
        self.defval = default_value
        self.min_limit = min_limit
        self.max_limit = max_limit

    def update(self, param):
        param.update({'num_prop': {
            'default_value': self.defval,
            'min_limit': self.min_limit,
            'max_limit': self.max_limit}})
        param.update({'num_value': int(self.defval)})


def registerProperty(name, type_info, edit_by=EDIT_BY_RESELLER, is_visible=None, defval=None):
    api = _prepareAPI()
    args = {'name': name, 'edit_by': edit_by}
    type_info.update(args)
    api.pem.registerSystemProperty(**args)


def setPropertyValue(name, value, account_id=1):
    api = _prepareAPI()
    args = {'account_id': account_id, 'name': name}
    value.update(args)
    api.pem.setSystemProperty(**args)


def setPropertyVisibility(name, edit_by):
    api = _prepareAPI()
    api.pem.setPropertyVisibility(name=name, edit_by=edit_by)
