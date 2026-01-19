from __future__ import annotations

from dataclasses import dataclass as _dc
from dataclasses import field
from typing import Any, dataclass_transform


class LazyMeta:
    """Descriptor that lazily creates and caches the Meta object on first access."""

    def __get__(self, obj: Any, cls: type):
        from tuplesaver.model import make_model_meta

        meta = make_model_meta(cls)
        cls._meta = meta
        return meta


@dataclass_transform()
class RowMeta(type):
    """Metaclass that transforms classes into frozen dataclasses."""

    def __new__(cls, typename: str, bases: tuple[type, ...], ns: dict[str, Any]) -> type:
        new_cls = super().__new__(cls, typename, bases, ns)

        # Apply dataclass decorator (frozen for immutability)
        # the id field in Roww base uses field(kw_only=True) to avoid
        # "non-default follows default" errors
        if "__dataclass_fields__" not in new_cls.__dict__:
            new_cls = _dc(new_cls, frozen=True)

        # Add lazy _meta descriptor
        new_cls._meta = LazyMeta()  # type: ignore[attr-defined]

        # Add _lazy_meta marker for is_row_model detection before _meta is accessed
        new_cls._lazy_meta = True  # type: ignore[attr-defined]

        return new_cls


class Roww(metaclass=RowMeta):
    id: int | None = field(default=None, kw_only=True)
