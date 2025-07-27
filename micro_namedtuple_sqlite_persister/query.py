from __future__ import annotations

import ast
import inspect
from functools import wraps
from textwrap import dedent
from typing import Any, Callable

from micro_namedtuple_sqlite_persister.model import Row, get_meta


class QueryError(Exception):
    pass


class SelectDual[R: Row](tuple[type[R], str]):
    def __new__(cls, Model: type[R]) -> SelectDual[R]:
        # Create a tuple (Model, meta.select)
        meta = get_meta(Model)
        assert meta.select is not None, "Model must have a select statement defined."
        return super().__new__(cls, (Model, meta.select))

    def __call__[**P](self, func: Callable[P, Any]) -> Callable[P, tuple[type[R], str, dict[str, Any]]]:
        q = render_query_def_func(self[0], func)
        argnames = inspect.signature(func).parameters.keys()

        @wraps(func)
        def wrapper(*args, **kwargs) -> tuple[type[R], str, dict[str, Any]]:
            combined_kwargs = {**dict(zip(argnames, args)), **kwargs}
            return (self[0], q, combined_kwargs)

        return wrapper


def select[R: Row](Model: type[R]) -> SelectDual[R]:
    return SelectDual.__new__(SelectDual, Model)


def render_query_def_func(Model: type[Row], func: Callable) -> str:
    source = inspect.getsource(func)
    source = dedent(source)
    tree = ast.parse(source)
    fbody = tree.body[0].body  # type: ignore
    assert len(fbody) == 1, "The query def must only contain a single expression"
    expr = fbody[0]

    js: ast.JoinedStr = expr.value
    query_parts: list[str] = []
    joins: dict[str, str] = {}

    parameter_names = set(inspect.signature(func).parameters.keys())
    unused_parameters = parameter_names.copy()

    basemeta = get_meta(Model)
    for v in js.values:
        match v:
            case ast.Constant():
                assert isinstance(v.value, str), "This is supposed to be a string part of an f-string"
                query_parts.append(v.value)
            case ast.FormattedValue():
                # We could use formats and conversions to do special things later if we want
                assert v.format_spec is None, "Do not include format specifiers in the field names. e.g. {name:0.2f}"
                assert v.conversion == -1, "Do not include conversion  in the field names. e.g. {!r}"

                stack = []
                for n in ast.walk(v.value):
                    if isinstance(n, (ast.Attribute, ast.Name)):
                        stack.append(n)
                    elif isinstance(n, ast.Load):
                        pass
                    else:
                        raise QueryError(f"All formatted values e.g. within `{{...}}`, must be either Fields of Models or parameters, not {n}: {type(n)}")
                stack.reverse()
                assert isinstance(stack[0], ast.Name), "The first part of the field name must be a class name"
                assert all(isinstance(n, ast.Attribute) for n in stack[1:]), "The rest of the field name must be attributes"

                name = stack[0].id
                if Model.__name__ == name:
                    ModelName = name
                    # This is correct root model for a field specification
                    found_intermediatte_field = None
                    if len(stack) > 2:
                        join_alias_parts = []
                        meta = basemeta
                        last_jalias = basemeta.table_name
                        for attrlevel in stack[1:-1]:
                            join_alias_parts.append(attrlevel.attr)
                            # TODO: save this looping by add a field by name dict to meta
                            for field in meta.fields:
                                if field.name == attrlevel.attr:
                                    found_intermediatte_field = field
                                    meta = get_meta(field.type)
                                    jalias = "_".join(join_alias_parts)
                                    joins[jalias] = f"JOIN {meta.table_name} {jalias} ON {last_jalias}.{field.name} = {jalias}.id"
                                    last_jalias = "_".join(join_alias_parts)

                                    break
                            assert found_intermediatte_field is not None, f"Field {attrlevel.attr} not found in {ModelName}"
                        jalias = "_".join(join_alias_parts)
                        table_or_alias = jalias
                        finalmeta = meta
                    else:
                        assert basemeta.table_name is not None, "Base model must have a table name defined."
                        table_or_alias = basemeta.table_name
                        finalmeta = basemeta
                    final_field = None
                    # TODO: save this looping by add a field by name dict to meta
                    for field in finalmeta.fields:
                        if field.name == stack[-1].attr:
                            final_field = field
                            break
                    assert final_field is not None, f"Field {stack[-1].attr} not found in {ModelName}"
                    query_parts.append(table_or_alias + "." + final_field.name)
                elif name in parameter_names:
                    query_parts.append(f":{name}")
                    unused_parameters.remove(name)
                else:
                    raise QueryError(f"Specify all columns as field paths from {Model.__name__}, e.g. {Model.__name__}.foo.bar.name")
            case _:
                print("Unknown type", type(v), v)
                raise QueryError("Unknown type in query f-string")

    if len(unused_parameters) > 0:
        raise QueryError(f"Unused parameter(s): {', '.join(unused_parameters)}")

    meta = get_meta(Model)
    assert meta.select is not None, "Model must have a select statement defined."
    select = meta.select
    query_predicate = dedent("".join(query_parts)).strip()
    query_parts = [query_predicate]

    join_clauses = joins.values()
    if len(join_clauses) > 0:
        query_parts.insert(0, "\n".join(join_clauses) + "\n")
    query_parts.insert(0, select + "\n")
    return "".join(query_parts)
