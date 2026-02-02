"""Models for fresh_db_with_model scenario."""

from tuplesaver.model import TableRow


class User(TableRow):
    name: str
    email: str
