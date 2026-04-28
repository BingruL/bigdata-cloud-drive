"""Shared route helpers."""
from flask import request


class BadArg(ValueError):
    """Raised when a query parameter fails validation."""


def parse_int_arg(name, default=None, min_value=None, max_value=None, required=False):
    """Parse a query string integer with bounds. Raises BadArg on invalid input.

    Routes catch BadArg and translate to a 400 response.
    """
    raw = request.args.get(name)
    if raw is None or raw == "":
        if required:
            raise BadArg(f"缺少参数 {name}")
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise BadArg(f"参数 {name} 必须是整数")
    if min_value is not None and value < min_value:
        raise BadArg(f"参数 {name} 不能小于 {min_value}")
    if max_value is not None and value > max_value:
        raise BadArg(f"参数 {name} 不能大于 {max_value}")
    return value
