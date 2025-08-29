from typing import dataclass_transform, TypeVar, Protocol
from typing import ParamSpec, Self, assert_type

from dataclasses import dataclass as _dc


@dataclass_transform()
class ModelMeta(type):
    id: int | None = None

    def __new__(mcls, name, bases, namespace):
        ann: dict = dict(namespace.get("__annotations__", {}))
        if not ann:
            return super().__new__(mcls, name, bases, namespace)

        cls = super().__new__(mcls, name, bases, dict(namespace))
        cls = _dc(cls)  # type: ignore

        @classmethod
        def from_identified_args(cls_, ident, /, *args, **kwargs):
            obj = cls_(*args, **kwargs)
            obj.id = ident
            return obj


        cls.from_identified_args = from_identified_args  # type: ignore[attr-defined]
        cls.from_identified_tuple = from_identified_tuple  # type: ignore[attr-defined]

        # if not any("id" in c.__dict__.get("__annotations__", {}) for c in [cls] + list(cls.__mro__[1:])):
        #     setattr(cls, "id", None)

        return cls

P = ParamSpec("P")
T = TypeVar("T", bound="ModelBase")
class _Ctor(Protocol[P, T]): #type: ignore for now
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...

class ModelBase(metaclass=ModelMeta):
    # Fully type-checked varargs constructor based on the dataclass __init__
    @classmethod #type: ignore for now
    def from_identified_args(cls: _Ctor[P, Self], ident: int | None, /, *args: P.args, **kwargs: P.kwargs) -> Self: ...


class MyRowDCT(ModelBase):
    name: str
    active: bool


car = MyRowDCT(name="Car", active=True)
car = MyRowDCT("Car", True)
assert_type(car.id, int | None)
assert_type(car.name, str)
assert_type(car.active, bool)

assert car.id == 100
assert car.name == "Car"
assert car.active is True

car = MyRowDCT.from_identified_args(101, "SUV", True)
assert car.id == 101
assert car.name == "SUV"
assert car.active is True


print(car)
print(MyRowDCT.id)
print(MyRowDCT.name)
