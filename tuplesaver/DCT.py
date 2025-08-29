from typing import Any, dataclass_transform, TypeVar, Protocol
from typing import ParamSpec, Self, assert_type

from dataclasses import dataclass as _dc

class Column():
    def __init__(self, name: str, type: type):
        self.name = name
        self.type = type
    def __repr__(self):
        return f"Column(name={self.name}, type={self.type})"


@dataclass_transform()
class ModelMeta(type):
    id: int | None = None
    __columnss__: dict[str, Column]


    def __new__(mcls, name, bases, namespace):
        ann: dict = dict(namespace.get("__annotations__", {}))
        if not ann:
            return super().__new__(mcls, name, bases, namespace)

        # Add id field to annotations before creating dataclass
        ann["id"] = int | None
        namespace["__annotations__"] = ann
        # Set default value for id
        namespace["id"] = None

        cls = super().__new__(mcls, name, bases, dict(namespace))
        cls = _dc(cls)  # type: ignore

        @classmethod
        def from_identified_args(cls_, ident, /, *args, **kwargs):
            obj = cls_(*args, **kwargs)
            obj.id = ident
            return obj


        cls.from_identified_args = from_identified_args  # type: ignore[attr-defined]

        cls.__columnss__ = {}

        # Add all fields (including id) to columnss
        for field_name, field_type in ann.items():
            cls.__columnss__[field_name] = Column(name=field_name, type=field_type)

        return cls

    def __getattribute__(self, name: str) -> Any:
        # First try to get the attribute normally
        try:
            return super().__getattribute__(name)
        except AttributeError:
            # If that fails, check if it's a column name
            columnss = super().__getattribute__("__columnss__")
            if name in columnss:
                print(f"Returning Column object for {name}")
                return columnss[name]
            raise AttributeError(f"'{self.__name__}' class has no attribute '{name}'")

P = ParamSpec("P")
T = TypeVar("T", bound="ModelBase")
class _Ctor(Protocol[P, T]): #type: ignore for now
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...

class ModelBase(metaclass=ModelMeta):
    # Fully type-checked varargs constructor based on the dataclass __init__
    @classmethod #type: ignore for now
    def from_identified_args(cls: _Ctor[P, Self], ident: int | None, /, *args: P.args, **kwargs: P.kwargs) -> Self: ... #type: ignore for now



class MyRowDCT(ModelBase):
    name: str
    active: bool


# KWARGS
car = MyRowDCT(name="Car", active=True)
# ARGS
car = MyRowDCT("Car", True)
assert_type(car.id, int | None)
assert_type(car.name, str)
assert_type(car.active, bool)

assert car.id == None
assert car.name == "Car"
assert car.active is True

car = MyRowDCT.from_identified_args(101, "SUV", True)
assert car.id == 101
assert car.name == "SUV"
assert car.active is True


print(MyRowDCT.id)
print(MyRowDCT.name)
