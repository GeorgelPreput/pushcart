"""Functions for making HTTP requests.

Supports different types of authentication (OAuth, Basic, No Auth) and request methods
(GET, POST). The main functions provided are batch_request for making batch requests
and streaming_request for downloading large files.
"""
import contextlib
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse, urlunparse

import httpx
from authlib.integrations.httpx_client import OAuth2Client
from authlib.oauth2.rfc7523 import ClientSecretJWT
from requests.structures import CaseInsensitiveDict
from tqdm import tqdm


def _build_oauth_client(auth: dict) -> httpx.Client:
    """Create an OAuth2 client using the given authentication information.

    Parameters
    ----------
    auth : dict
        Authentication details for getting OAuth2 token.

    Returns
    -------
    httpx.Client
        Client object to run the requests from.

    Raises
    ------
    ValueError
        Auth should not be null, and the fields token-url, client-id and
        client-secret must exist.
    """
    if auth is None:
        msg = "Please provide a valid auth specification"
        raise ValueError(msg)
    if not (token_url := auth.get("token-url")):
        msg = "Please provide a token-url in the auth specification"
        raise ValueError(msg)
    if not (client_id := auth.get("client-id")):
        msg = "Please provide a client-id in the auth specification"
        raise ValueError(msg)
    if not (client_secret := auth.get("client-secret")):
        msg = "Please provide a client-secret in the auth specification"
        raise ValueError(msg)

    client = OAuth2Client(
        client_id=client_id,
        client_secret=client_secret,
        token_endpoint_auth_method="client_secret_jwt",  # noqa: S106
    )
    client.register_client_auth_method(ClientSecretJWT(token_endpoint=token_url))
    client.fetch_token(token_url)

    return client


def _build_basic_auth_client(auth: dict) -> httpx.Client:
    """Create a Basic Auth client using the given authentication information.

    Parameters
    ----------
    auth : dict, optional
        Authentication details such as username and password.

    Returns
    -------
    httpx.Client
        Client object to run the requests from.

    Raises
    ------
    ValueError
        Auth should not be null, and the client-id and client-secret fields must exist.
    """
    if auth is None:
        msg = "Please provide a valid auth specification"
        raise ValueError(msg)
    if not (client_id := auth.get("client-id")):
        msg = "Please provide a client-id in the auth specification"
        raise ValueError(msg)
    if not (client_secret := auth.get("client-secret")):
        msg = "Please provide a client-secret in the auth specification"
        raise ValueError(msg)

    basic_auth = httpx.BasicAuth(username=client_id, password=client_secret)
    transport = httpx.HTTPTransport(retries=5)
    return httpx.Client(transport=transport, auth=basic_auth)


def _build_no_auth_client() -> httpx.Client:
    """Create a client with no authentication.

    Returns
    -------
    httpx.Client
        Client object to run the requests from.
    """
    transport = httpx.HTTPTransport(retries=5)
    return httpx.Client(transport=transport)


def _build_client(auth: dict) -> httpx.Client:
    """Create a client with the given authentication information.

    Parameters
    ----------
    auth : dict
        Authentication details.

    Returns
    -------
    httpx.Client
        Client object to run the requests from.

    Raises
    ------
    ValueError
        Auth should not be null, and the field type within should have one of the
        values: NO_AUTH, BASIC, OAUTH2
    """
    if auth is None:
        msg = "Please specify an authentication type"
        raise ValueError(msg)

    auth_type = str(auth.get("type")).upper()

    if auth_type == "NO_AUTH":
        return _build_no_auth_client()
    if auth_type == "BASIC":
        return _build_basic_auth_client(auth)
    if auth_type == "OAUTH2":
        return _build_oauth_client(auth)

    msg = f"Expected auth['type'] in ['NO_AUTH', 'BASIC', 'OAUTH2']. Got: {auth_type}"
    raise ValueError(msg)


def _split_params_from_url(url: str) -> tuple[str, dict]:
    """Split the URL into the base URL and the query parameters.

    Parameters
    ----------
    url : str
        Original URL, possibly containing GET request parameters.

    Returns
    -------
    tuple[str, dict]
        Tuple containing the URL string, stripped of parameters on position 0, and a
        parameter dictionary derived from the URL on position 1.
    """
    parsed_url = urlparse(url)
    derived_params = parse_qs(parsed_url.query)
    stripped_url = urlunparse(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            "",
            "",
            parsed_url.fragment,
        ),
    )

    return stripped_url, derived_params


def _update_url_and_params(url: str, params: dict) -> tuple[str, dict]:
    """Update the URL and its parameters with the given parameters.

    Parameters
    ----------
    url : str
        Original URL, possibly containing GET request parameters.
    params : dict
        Explicit parameters which override whatever might be derived from the URL.

    Returns
    -------
    tuple[str, dict]
        Tuple containing the URL string, stripped of parameters, and a dictionary
        containing a merge of the parameters derived from the URL and the explicit
        parameters, where the explicit parameters override any derived ones.
    """
    stripped_url, derived_params = _split_params_from_url(url)
    derived_params.update(params)

    return stripped_url, derived_params


def _add_data_for_get_request(data: str, json_data: dict) -> dict:
    """Prepare the data field sent in the body of a GET request.

    Parameters
    ----------
    data : str
        Text or dictionary to be sent over as data in the request.
    json_data : dict
        Text or dictionary to be sent over as data in the request.

    Returns
    -------
    dict
        Dictionary containing the data prepared to be sent as a
        keyword argument.

    Raises
    ------
    TypeError
        Provided data can only be a string or dict.
    """
    kwargs = {}

    if data is None:
        return kwargs

    if isinstance(json_data, dict):
        kwargs["data"] = json.dumps(json_data)
        return kwargs

    if isinstance(data, str):
        kwargs["data"] = data
        return kwargs

    type_error_msg = f"Expected either data to be str or json_data to be dict. Got data: {type(data)} and json_data: {type(json_data)}"
    raise TypeError(type_error_msg)


def _add_data_for_post_request(data: str, json_data: dict) -> dict:
    """Prepare the data field sent in the body of a POST request.

    Parameters
    ----------
    data : str
        Text to be sent over as data in the request.
    json_data : dict
        Dictionary to be sent over as data in the request.

    Returns
    -------
    dict
        Dictionary containing the data prepared to be sent as a
        keyword argument.

    Raises
    ------
    TypeError
        Provided data can only be a string or dict.
    """
    kwargs = {}

    if data is None and json_data is None:
        return kwargs

    if isinstance(json_data, dict):
        kwargs["json"] = json_data
        return kwargs

    if isinstance(data, str):
        kwargs["data"] = data
        return kwargs

    type_error_msg = f"Expected either data to be str or json_data to be dict. Got data: {type(data)} and json_data: {type(json_data)}"
    raise TypeError(type_error_msg)


def _prepare_request(  # noqa: PLR0913
    url: str,
    headers: dict,
    params: dict,
    options: dict,
    data: str,
    json_data: dict,
    method: str,
) -> dict:
    """Prepare the GET or POST request with the given parameters.

    Parameters
    ----------
    url : str
        URL of the endpoint
    headers : dict
        Headers to be sent over
    params : dict
        Parameters to be sent as part of the request
    options : dict
        Dictionary of options to send to the httpx.Client request function. For more info,
        see: https://www.python-httpx.org/api/
    data : str
        Text data to be sent as part of the request.
    json_data : dict
        JSON data to be sent as part of the request.
    method : str
        GET or POST

    Returns
    -------
    dict
        Dictionary of keyword arguments to be passed to the httpx.request function.
    """
    kwargs = {"url": url, "headers": headers, "params": params, "method": method}
    kwargs.update(options)

    if method.upper() == "GET":
        kwargs.update(_add_data_for_get_request(data, json_data))

    if method.upper() == "POST":
        kwargs.update(_add_data_for_post_request(data, json_data))

    return kwargs


def _parse_api_response(res: httpx.Response) -> list[dict]:
    """Parse the response from the API and returns it as a list of dictionaries.

    Parameters
    ----------
    res : httpx.Response
        HTTP response containing data.

    Returns
    -------
    list[dict]
        Data payload from the HTTP response, formatted as a list of dictionaries.
    """
    res_json = None

    with contextlib.suppress(json.JSONDecodeError):
        res_json = res.json()

    if not res_json:
        res_json = {"payload": res.text}

    if not isinstance(res_json, list):
        res_json = [res_json]

    return res_json


def batch_request(  # noqa: PLR0913
    url: str,
    headers: dict = None,
    auth: dict = None,
    params: dict = None,
    data: str = None,
    json_data: dict = None,
    method: str = "GET",
    options: dict = None,
) -> list[dict]:
    """Send a batch request and returns the parsed response.

    Parameters
    ----------
    url : str
        REST API endpoint URL
    headers : dict, optional
        Headers to be sent, by default None
    auth : dict, optional
        Authentication method, by default None
    params : dict, optional
        Parameters to be sent, by default None
    data : str, optional
        Text data to be sent in the body of the request, by default None
    json_data : dict, optional
        JSON data to be sent in the body of the request, by default None
    method : str, optional
        "GET" or "POST", by default "GET"
    options : dict, optional
        Parameters to be sent to the httpx.Client request function, by default None.
        For more info, see: https://www.python-httpx.org/api/

    Returns
    -------
    list[dict]
        Data payload from the HTTP response, formatted as a list of dictionaries.
    """
    headers = headers or {}
    params = params or {}
    # For options, see: https://www.python-httpx.org/api/
    options = options or {}
    auth = auth or {"type": "NO_AUTH"}

    stripped_url, stripped_params = _update_url_and_params(url=url, params=params)
    client = _build_client(auth)

    kwargs = _prepare_request(
        url=stripped_url,
        headers=headers,
        params=stripped_params,
        options=options,
        data=data,
        json_data=json_data,
        method=method,
    )

    res = client.request(**kwargs)

    return _parse_api_response(res)


def streaming_request(  # noqa: PLR0913
    target_path: str,
    url: str,
    headers: dict = None,
    auth: dict = None,
    params: dict = None,
    data: str = None,
    json_data: dict = None,
    method: str = "GET",
    options: dict = None,
) -> str:
    """Send a streaming request and save the response to the target path.

    Parameters
    ----------
    target_path : str
        File path to stream HTTP response to.
    url : str
        REST API endpoint URL
    headers : dict, optional
        Headers to be sent, by default None
    auth : dict, optional
        Authentication method, by default None
    params : dict, optional
        Parameters to be sent, by default None
    data : str, optional
        Text data to be sent in the body of the request, by default None
    json_data : dict, optional
        JSON data to be sent in the body of the request, by default None
    method : str, optional
        "GET" or "POST", by default "GET"
    options : dict, optional
        Parameters to be sent to the httpx.Client request function, by default None.
        For more info, see: https://www.python-httpx.org/api/

    Returns
    -------
    str
        File path to which the HTTP response was streamed to.
    """
    headers = headers or {}
    params = params or {}
    options = options or {}
    auth = auth or {"type": "NO_AUTH"}

    stripped_url, stripped_params = _update_url_and_params(url=url, params=params)
    client = _build_client(auth)

    kwargs = _prepare_request(
        url=stripped_url,
        headers=headers,
        params=stripped_params,
        options=options,
        data=data,
        json_data=json_data,
        method=method,
    )
    with Path(target_path).open("+wb") as download_file, client.stream(**kwargs) as res:
        res_headers = CaseInsensitiveDict(res.headers)
        total = int(res_headers.get("content-length", "0"))

        with tqdm(
            total=total,
            unit_scale=True,
            unit_divisor=1024,
            unit="B",
        ) as progress:
            num_bytes_downloaded = res.num_bytes_downloaded
            for chunk in res.iter_bytes():
                download_file.write(chunk)
                progress.update(res.num_bytes_downloaded - num_bytes_downloaded)
                num_bytes_downloaded = res.num_bytes_downloaded

    return target_path
