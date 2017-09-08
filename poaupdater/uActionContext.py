"""This module keeps context within running atomic action and allows commit/rollback semantics for non-trivial entities.
"""
__rcs_id__ = """$Id$"""
__pychecker__ = "unusednames=__rcs_id__,dummy"

from uAction import nothrowable

_action_context = {}


def get(name):
    """Return object from atomic action context by its name, if exists or None."""
    global _action_context
    return _action_context.get(name)


def put(name, val):
    """Put an object to atomic action context. If object has commit and
    rollback methods then those method would be called on successful action
    exit or failure respectively."""
    global _action_context
    _action_context[name] = val


def prepare(action_id, precheck=False):
    """Re-create context for current atomic action.
    NOTE: Do not call it, this is called automatically."""
    _clear()
    put('action_id', action_id)
    put('precheck', precheck)


def action_id():
    """Get current atomic action ID"""
    return get('action_id')


def in_precheck():
    """Get current atomic action precheck mode"""
    return get('precheck')


def _clear():
    global _action_context
    _action_context.clear()


def _finish(fn_name, fn_wrap):
    global _action_context

    for k, v in _action_context.items():
        if hasattr(v, fn_name):
            m = getattr(v, fn_name)
            fn_wrap(m)

    _clear()


def commit():
    w = lambda f: f()
    _finish('commit', w)


def rollback():
    w = lambda f: nothrowable(f)()
    _finish('rollback', w)
