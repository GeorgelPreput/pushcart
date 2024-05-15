"""Utility functions for PushCart."""

import re


def _normalize_old(s: str, ignore_case: bool = False) -> str:
    return s.lower() if ignore_case else s


def multireplace(string: str, replacements: dict, ignore_case: bool = False) -> str:
    """Apply multiple replacement patterns in a string.

    Parameters
    ----------
    string : str
        Original string to process
    replacements : dict
        Map of substrings that have to be replaced
    ignore_case : bool, optional
        Whether to apply replacements ignoring case, by default False

    Returns
    -------
    str
        New string with substrings replaced

    """
    if not replacements:
        return string

    replacements = {_normalize_old(key): val for key, val in replacements.items()}

    # Place longer strings first to keep shorter substrings from matching where the
    # longer ones should be replaced. For instance, given the replacement map
    # {"ab": "AB", "abc", "ABC"} for the string "hey abc", it should produce
    # "hey ABC" and not "hey ABc"
    rep_sorted = sorted(replacements, key=len, reverse=True)
    rep_escaped = [str(escaped) for escaped in map(re.escape, rep_sorted)]

    pattern = re.compile("|".join(rep_escaped), re.IGNORECASE if ignore_case else 0)

    return pattern.sub(
        lambda match: replacements[_normalize_old(match.group(0))],
        string,
    )


def pandas_to_spark_datetime_pattern(pattern: str) -> str:
    """Convert a Pandas datetime format pattern to a Spark datetime format pattern.

    This function takes a datetime format pattern used in Pandas and converts it
    to a corresponding format pattern that can be used with Spark. It performs this
    conversion using a predefined mapping of Pandas pattern tokens to Spark pattern tokens.

    Parameters
    ----------
    pattern : str
        The Pandas datetime format pattern string that needs to be converted.
        For example: "%Y-%m-%d %H:%M:%S".

    Returns
    -------
    str
        The converted Spark datetime format pattern string.
        For example, given the input pattern "%Y-%m-%d %H:%M:%S",
        the function would return "yyyy-MM-dd HH:mm:ss".

    """
    replacement_map = {
        "%Y": "yyyy",
        "%y": "yy",
        "%m": "MM",
        "%B": "MMMM",
        "%b": "MMM",
        "%d": "dd",
        "%A": "EEEE",
        "%a": "EEE",
        "%H": "HH",
        "%I": "hh",
        "%p": "a",
        "%M": "mm",
        "%S": "ss",
        "%f": "SSSSSS",
        "%z": "Z",
        "%Z": "z",
        "T": "'T'",
    }

    return multireplace(pattern, replacement_map)
