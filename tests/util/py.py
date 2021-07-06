def dump(obj):
    """ Dump an object's type and fields. Applicable to both dicts and slots """
    if type(obj).__name__ in PRIMITIVE_TYPE_NAMES:
        return obj
    if hasattr(obj, '__dict__'):
        return type(obj), vars(obj)
    else:
        return type(obj), {
            k: getattr(obj, k)
            for k in dir(obj)
            if not (k.startswith('__') and k.endswith('__'))
        }


import builtins
PRIMITIVE_TYPE_NAMES = dir(builtins)
