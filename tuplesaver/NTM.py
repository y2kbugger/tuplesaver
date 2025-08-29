from typing import NamedTuple, TypeAlias
from typing import NamedTupleMeta # type: ignore this is not in typeshed
from typing import Any

class Column():
    def __init__(self, name: str, type: type):
        self.name = name
        self.type = type
    def __repr__(self):
        return f"Column(name={self.name}, type={self.type})"

class META(NamedTupleMeta):...


class MY_ROW(NamedTuple, metaclass=META):
    id: int
    name: str
    active: bool

assert MY_ROW.id is Column(name="id", type=int )
assert MY_ROW.name is Column(name="name", type=str )
car = MY_ROW(id=1, name="Toyota", active=True)
assert car.id == 1
