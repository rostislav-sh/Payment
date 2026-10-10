"""Short, single-place text of an error for logs and stored failure reasons."""


def describe_error(exc: Exception) -> str:
    """``Type: message``, followed by the direct cause if there is one."""
    cause = exc.__cause__
    detail = f"{type(cause).__name__}: {cause}" if cause is not None else str(exc)
    return f"{type(exc).__name__}: {detail}"
