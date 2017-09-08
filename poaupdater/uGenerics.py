
def first(iterable, condition=None):
    if condition is None:
        condition = bool
    for i in iterable:
        if condition(i):
            return i

    return None


def trueCondition(dummy):
    return True


def categorize_uni(iterable, cat, condition=None):
    rv = {}
    if condition is None:
        condition = trueCondition
    for i in iterable:
        if condition(i):
            rv[cat(i)] = i

    return rv


def categorize(iterable, cat, condition=None):
    rv = {}
    if condition is None:
        condition = trueCondition
    for i in iterable:
        if condition(i):
            category = cat(i)
            if rv.has_key(category):
                rv[category].append(i)
            else:
                rv[cat(i)] = [i]

    return rv


__all__ = ['first', 'categorize', 'categorize_uni']
