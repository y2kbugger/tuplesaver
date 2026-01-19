from dataclasses import dataclass as _dc
from dataclasses import field
from typing import Any, assert_type, dataclass_transform


class Column:
    def __init__(self, name: str, type: type):
        self.name = name
        self.type = type

    def __repr__(self):
        return f"Column(name={self.name}, type={self.type})"


@dataclass_transform()
class ModelMeta(type):
    __columnss__: dict[str, Column]

    def __new__(mcls, name, bases, namespace):  # noqa: ANN001
        cls = super().__new__(mcls, name, bases, namespace)

        # Only apply dataclass if not already a dataclass (avoid recursion from slots=True)
        # Check __dict__ directly to avoid finding inherited __dataclass_fields__
        if '__dataclass_fields__' not in cls.__dict__:
            cls = _dc(cls, slots=True, frozen=True)

        # Merge columnss in from base classes, e.g. at least the id field from ModelBase.
        cls.__columnss__ = {}
        for base in bases:
            cls.__columnss__.update(getattr(base, "__columnss__", {}))

        ann: dict = dict(namespace.get("__annotations__", {}))

        # Add all fields from the current class to columnss
        for field_name, field_type in ann.items():
            cls.__columnss__[field_name] = Column(name=field_name, type=field_type)

        return cls

    def __getattr__(self, name: str) -> Any:
        # Only called when attribute is NOT found normally
        print(f"%%getattr called for: {name}")
        # Use object.__getattribute__ to avoid recursion if __columnss__ doesn't exist yet
        try:
            columnss = object.__getattribute__(self, "__columnss__")
        except AttributeError:
            raise AttributeError(f"'{self.__name__}' class has no attribute '{name}'") from None
        if name in columnss:
            print(f"\tFound column {name} in __columnss__, returning it")
            return columnss[name]
        raise AttributeError(f"'{self.__name__}' class has no attribute '{name}'")


class ModelBase(metaclass=ModelMeta):
    id: int | None = field(default=None, kw_only=True)


class Car(ModelBase):
    make: str
    year: int


# KWARGS
car = Car(make="Honda", year=2020)
assert_type(car, Car)
assert_type(car.id, int | None)
assert_type(car.make, str)
assert_type(car.year, int)
assert car.id is None
assert car.make == "Honda"
assert car.year == 2020

# ARGS
car2 = Car("Toyota", 2017)
assert_type(car2.id, int | None)
assert_type(car2.make, str)
assert_type(car2.year, int)
assert car2.id is None
assert car2.make == "Toyota"
assert car2.year == 2017


car3 = Car("Hyundai", 1999, id=101)
assert_type(car3.id, int | None)
assert_type(car3.make, str)
assert_type(car3.year, int)
assert car3.id == 101
assert car3.make == "Hyundai"
assert car3.year == 1999

print("\nClass level shits:")
print(Car.make)
print(Car.year)
print(Car.id)
print(Car.__columnss__)
print(Car.__columnss__["make"])
print(Car.__columnss__["year"])
print(Car.__columnss__["id"])
