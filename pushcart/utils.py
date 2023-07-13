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
    rep_escaped = map(re.escape, rep_sorted)

    pattern = re.compile("|".join(rep_escaped), re.IGNORECASE if ignore_case else 0)

    return pattern.sub(
        lambda match: replacements[_normalize_old(match.group(0))],
        string,
    )
