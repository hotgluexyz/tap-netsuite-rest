import socket
from hotglue_etl_exceptions import InvalidCredentialsError

_NETSUITE_CONTROL_HOST = "netsuite.com"

def check_netsuite_dns_failure(e):
    if not _is_netsuite_subdomain_dns_failure(e):
        return
    if not _control_host_resolves():
        return
    raise InvalidCredentialsError(
        f"Please verify your account ID is correct. Error: {str(e)}"
    ) from e


def _is_netsuite_subdomain_dns_failure(e):
    seen = set()
    queue = [e]
    while queue:
        current = queue.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        if type(current).__name__ == "NameResolutionError":
            failed_host = getattr(current, "_host", "") or ""
            return "suitetalk.api.netsuite.com" in failed_host
        if getattr(current, "__cause__", None) is not None:
            queue.append(current.__cause__)
        if getattr(current, "__context__", None) is not None:
            queue.append(current.__context__)
    return False


def _control_host_resolves():
    try:
        socket.gethostbyname(_NETSUITE_CONTROL_HOST)
        return True
    except OSError:
        return False
