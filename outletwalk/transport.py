from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

_DEFAULT_PORTS = {'http': 80, 'https': 443}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, target):
        return None


def origin_tuple(url):
    """Normalized (scheme, host, port) identity, filling scheme-default ports."""
    parsed = urlsplit(url)
    port = parsed.port
    if port is None and parsed.scheme in _DEFAULT_PORTS:
        port = _DEFAULT_PORTS[parsed.scheme]
    return parsed.scheme, parsed.hostname, port


def _open(url, allowed_hosts, maximum, timeout):
    parsed = urlsplit(url)
    if parsed.scheme != 'http' or parsed.hostname not in allowed_hosts:
        raise ValueError('The request target is outside the allowed origin')
    if parsed.username or parsed.password:
        raise ValueError('Credentials in URLs are not accepted')
    request = Request(url, method='GET', headers={})
    opener = build_opener(ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(request, timeout=timeout)
    except HTTPError as error:
        response = error
    with response:
        body = response.read(maximum + 1)
        if len(body) > maximum:
            raise ValueError('Inspection response exceeds byte limit')
        return response.status, dict(response.headers.items()), body


def capture(url, method='GET', data=None, headers=None, maximum=4194304):
    return _open(url, ('127.0.0.1', 'localhost', '::1'), maximum, 3)


def fetch(url, allowed_origin, maximum=4194304, timeout=10):
    """GET a page while enforcing that the final request stays inside allowed_origin.

    No redirects are followed, so the origin of the response is always the origin
    of the requested URL; relative next links are resolved by callers.
    """
    origin = urlsplit(allowed_origin)
    if origin.scheme != 'http' or not origin.hostname or origin.username:
        raise ValueError('Invalid allowed origin configuration')
    if origin_tuple(url) != origin_tuple(allowed_origin):
        raise ValueError('The request target is outside the allowed origin')
    return _open(url, (origin.hostname,), maximum, timeout)
