# Docs/Notes
## JOINs
JOINs are automatic, and disambiguated by the the reference path `Athelete.team.name`
## Types
https://docs.python.org/3/library/sqlite3.html#sqlite3-types

These are the only built in type mappings

| Python | SQLite  |
|--------|---------|
| None   | NULL    |
| int    | INTEGER |
| float  | REAL    |
| str    | TEXT    |
| bytes  | BLOB    |

We also include by default the following mappings

| Python   | SQLite            | Adapt/Convert      |
|----------|-------------------|--------------------|
| bool     | builtins.bool     | 1 -> x01, 0 -> x00 |
| list     | builtins.list     | json dumps/loads   |
| dict     | builtins.dict     | json dumps/loads   |
| date     | datetime.date     | .isoformat()       |
| datetime | datetime.datetime | .isoformat()       |

Any other types will attempt to be pickled.


If you want to customize how you serialize, we use a thin wrapper on top of Adapt/Convert api.
https://docs.python.org/3/library/sqlite3.html#sqlite3-adapter-converter-recipes

    def adapt_date_iso(val):
        """Adapt datetime.date to ISO 8601 date."""
        return val.isoformat()

    sqlite3.register_adapter(datetime.date, adapt_date_iso)


    def convert_date(val):
        """Convert ISO 8601 date to datetime.date object."""
        return datetime.date.fromisoformat(val.decode())

    sqlite3.register_converter("date", convert_date)

So our api which wraps the above would look like this:

    def adapt_datetime_iso(val: datetime.datetime) -> bytes:
        """Adapt datetime.datetime to ISO 8601 date and time."""
        return val.isoformat().encode()

    def convert_datetime_iso(data: bytes) -> datetime.datetime:
        """Convert ISO 8601 date and time to datetime.datetime object."""
        return dt.datetime.fromisoformat(data.decode())

    from micro_namedtuple_sqlite_persister import register_adapt_convert

    register_adapt_convert(datetime.datetime, adapt_datetime_iso, convert_datetime_iso)

### A note on JSON columns
Currently we do not apply adapters/converters to json dumps, because without specifying a schema there is no way to reliably recover type info.

Even something as simple as date, would be ambiguous, because it could be a date or str going in

    {"date": "2021-01-01"}

If we enable dt.datetime serialization, then the above could have been

    dict(date=dt.datetime(2021, 1, 1))

or

    dict(date="2021-01-01")

## sqlite3
https://docs.python.org/3/library/sqlite3.html


Really need to read and understand this new annotation sematics coming in 3.14, as well as difference between inspect.get_nnotations and typing.get_type_hints
https://docs.python.org/3/howto/annotations.html#annotations-howto
https://github.com/python/cpython/issues/102405
https://peps.python.org/pep-0649/
