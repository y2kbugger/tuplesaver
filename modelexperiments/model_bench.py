"""
PURPOSE:
  Compare performance characteristics of candidate row model implementations

METRICS:
  - Model(1, 2): Time to instantiate an object with two int fields
  - m.a: Time to access an attribute on an instance
  - size: Memory footprint of a single instance (bytes via sys.getsizeof)

UNITS:
  - Time: microseconds (µs) - lower is better
  - Size: bytes (B) - lower is better
================================================================================

## Output Example:

Model(1, 2)	m.a	hash(m)	size	total size
dataclass	846.9	49.7	361.7	152	320
dataclass + slots	709.1	45.5	342.5	48	104
namedtuple	465.3	43.2	99.6	56	112

"""

import sys
from dataclasses import dataclass
from timeit import repeat
from typing import NamedTuple

import pandas as pd
from DCT import ModelBase
from NTM import RowMeta
from RM import Roww


class NT(NamedTuple):
    a: int
    b: int


class NTP(NamedTuple, metaclass=RowMeta):
    a: int
    b: int


class RM(Roww):
    a: int
    b: int


@dataclass()
class DC:
    a: int
    b: int


@dataclass(slots=True)
class DS:
    a: int
    b: int


@dataclass(frozen=True, slots=True)
class DFS:
    a: int
    b: int


class DCT(ModelBase):
    a: int
    b: int


MODEL_DISPLAY_NAMES = {
    NT: "NamedTuple",
    NTP: "NamedTuple, metaclass=RowMeta",
    RM: "Roww (Custom NamedTuple-Like BaseClass)",
    DC: "dataclass",
    DS: "dataclass + slots",
    DFS: "dataclass + frozen + slots",
    DCT: "DCT (Custom Dataclass-Like BaseClass)",
}
MODELS = [m for m in MODEL_DISPLAY_NAMES]
UNHASHABLE_MODELS = {DC, DS, DCT}
REPEATS = 10
NUMBER = 1_000_000
TEST_GLOBALS = {model.__name__: model for model in MODELS}

if __name__ == "__main__":
    times = {}
    sizes = {}
    results = {}
    for t in MODELS:
        tname = t.__name__
        print(f"Benchmarking {tname} instanciation")
        times[('isinstanciate', tname)] = min(repeat(f"{tname}(1,2)", number=NUMBER, repeat=REPEATS, globals=TEST_GLOBALS)) / NUMBER

        print(f"Benchmarking {tname} attribute access")
        times[('attr_access', tname)] = min(repeat("obj.a", setup=f"obj = {tname}(1, 2)", number=NUMBER, repeat=REPEATS, globals=TEST_GLOBALS)) / NUMBER

        if t not in UNHASHABLE_MODELS:
            print(f"Benchmarking {tname} hashing")
            times[('hashing', tname)] = min(repeat("hash(obj)", setup=f"obj = {tname}(1, 2)", number=NUMBER, repeat=REPEATS, globals=TEST_GLOBALS)) / NUMBER
        else:
            times[('hashing', tname)] = float('nan')

        print(f"Measuring {tname} size")
        sizes[('size', tname)] = sys.getsizeof(t(1, 2))

        display_name = MODEL_DISPLAY_NAMES[t]
        NS_PER_S = 1_000_000_000
        results[display_name] = {
            "Model(1, 2)": times[('isinstanciate', tname)] * NS_PER_S,
            "m.a": times[('attr_access', tname)] * NS_PER_S,
            "hash(m)": times[('hashing', tname)] * NS_PER_S,
            "size": sizes[('size', tname)],
        }

    print("units: nanoseconds (ns) per operation, bytes (B) for size")
    df = pd.DataFrame.from_dict(results, orient="index")
    print(df)
