def _is_empty(obj) -> bool:
    if isinstance(obj, str) and not obj.strip():
        return True
    if (
        isinstance(obj, dict)
        and not any(obj.values())
        and not any(isinstance(n, bool) for n in obj.values())
        and not any(isinstance(n, int) for n in obj.values())
    ):
        return True
    if (
        isinstance(obj, list)
        and not any(obj)
        and not any(isinstance(n, bool) for n in obj)
        and not any(isinstance(n, int) for n in obj)
    ):
        return True

    return False


def _sanitize_empty_elements(l: list, drop_empty=False) -> list:
    elements = [
        None
        if _is_empty(v)
        else _sanitize_empty_fields(v, drop_empty)
        if isinstance(v, dict)
        else _sanitize_empty_elements(v, drop_empty)
        if isinstance(v, list)
        else v
        for v in l
    ]
    if drop_empty:
        return [e for e in elements if e is not None]
    else:
        return elements


def _sanitize_empty_fields(d: dict, drop_empty=False) -> dict:
    fields = {
        k.replace(".", "_"): None
        if _is_empty(v)
        else _sanitize_empty_fields(v, drop_empty)
        if isinstance(v, dict)
        else _sanitize_empty_elements(v, drop_empty)
        if isinstance(v, list)
        else v
        for k, v in d.items()
    }
    if drop_empty:
        return {k: v for k, v in fields.items() if v is not None}
    else:
        return fields


def sanitize_empty_objects(o, drop_empty=False):
    if isinstance(o, list):
        return _sanitize_empty_elements(o, drop_empty)
    elif isinstance(o, dict):
        return _sanitize_empty_fields(o, drop_empty)
    else:
        raise TypeError(
            f"Can only sanitize lists or dictionaries. Got {type(o)}: {str(o)}"
        )
