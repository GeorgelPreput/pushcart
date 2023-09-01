"""Functions to sanitize objects by replacing or removing empty elements and fields. It handles nested lists and dictionaries."""
from typing import Any, Union


def _is_empty(obj: Any) -> bool:  # noqa: ANN401
    """Determine if an object is considered empty.

    Parameters
    ----------
    obj : Any
        The object to check.


    Returns
    -------
    bool
        True if the object is considered empty, False otherwise.
    """
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


def _sanitize_empty_elements(elem_list: list[Any], drop_empty: bool=False) -> list:
    """Sanitize empty elements within a list.

    Parameters
    ----------
    elem_list : list[Any]
        List of elements to sanitize.
    drop_empty : bool, optional
        If True, removes any empty elements from the list. Default is False.


    Returns
    -------
    list
        Sanitized list.
    """
    elements = [
        None
        if _is_empty(v)
        else _sanitize_empty_fields(v, drop_empty)
        if isinstance(v, dict)
        else _sanitize_empty_elements(v, drop_empty)
        if isinstance(v, list)
        else v
        for v in elem_list
    ]
    if drop_empty:
        return [e for e in elements if e is not None]

    return elements


def _sanitize_empty_fields(field_dict: dict, drop_empty: bool=False) -> dict:
    """Sanitize empty fields within a dictionary.

    Parameters
    ----------
    field_dict : dict
        Dictionary of fields to sanitize.
    drop_empty : bool, optional
        If True, removes any empty fields from the dictionary. Default is False.


    Returns
    -------
    dict
        Sanitized dictionary.
    """
    fields = {
        k.replace(".", "_"): None
        if _is_empty(v)
        else _sanitize_empty_fields(v, drop_empty)
        if isinstance(v, dict)
        else _sanitize_empty_elements(v, drop_empty)
        if isinstance(v, list)
        else v
        for k, v in field_dict.items()
    }
    if drop_empty:
        return {k: v for k, v in fields.items() if v is not None}

    return fields


def sanitize_empty_objects(obj: Union[list[Any], dict[Any, Any]], drop_empty: bool=False) -> Union[list[Any], dict[Any, Any]]:
    """Sanitize either a list or dictionary object.

    Parameters
    ----------
    obj : Union[list[Any], dict[Any, Any]]
        The object to sanitize. Can be either a list or dictionary.
    drop_empty : bool, optional
        If True, removes any empty elements or fields. Default is False.


    Returns
    -------
    Union[list[Any], dict[Any, Any]]
        Sanitized object.


    Raises
    ------
    TypeError
        If the input object is neither a list nor a dictionary.
    """
    if isinstance(obj, list):
        return _sanitize_empty_elements(obj, drop_empty)
    if isinstance(obj, dict):
        return _sanitize_empty_fields(obj, drop_empty)

    type_err_msg = f"Can only sanitize lists or dictionaries. Got {type(obj)}: {str(obj)}"
    raise TypeError(type_err_msg)
