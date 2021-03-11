from contextlib import suppress
from typing import Any


def format_with_default(val: str, default_val: Any, **kwargs: Any) -> Any:
  if not val:
    return default_val

  with suppress(KeyError): val = val.format(kwargs)

  return val if val else default_val
