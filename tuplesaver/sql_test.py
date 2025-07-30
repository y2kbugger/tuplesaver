from textwrap import dedent
from typing import NamedTuple

import pytest

from .model import get_meta
from .sql import QueryError, select


class League(NamedTuple):
    id: int | None
    leaguename: str


class Team(NamedTuple):
    id: int | None
    teamname: str
    league: League


class Athlete(NamedTuple):
    id: int | None
    name: str
    team: Team


@pytest.fixture(autouse=True)  # autouse for this test module
def ensure_meta_is_registered_in_the_correct_order() -> None:
    get_meta(League)
    get_meta(Team)
    get_meta(Athlete)


def dd(sql: str) -> str:
    return dedent(sql).strip()


def test_select_on_table() -> None:
    M, q = select(Athlete)

    assert q == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team FROM Athlete
        """)


def test_select_on_table_with_where() -> None:
    @select(Athlete)
    def athletes_named_joe():
        return f"WHERE {Athlete.name} = 'Joe'"

    M, q, _ = athletes_named_joe()
    assert q == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team FROM Athlete
        WHERE Athlete.name = 'Joe'
        """)


def test_select_on_table_with_join_caused_by_predicate() -> None:
    @select(Athlete)
    def athletes_on_red_team():
        return f"WHERE {Athlete.team.teamname} = 'Red Snickers'"

    M, q, _ = athletes_on_red_team()
    assert q == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team FROM Athlete
        JOIN Team team ON Athlete.team = team.id
        WHERE team.teamname = 'Red Snickers'
        """)


def test_select_on_table_with_multiple_implicit_joins() -> None:
    @select(Athlete)
    def athletes_in_big_league():
        return f"WHERE {Athlete.team.league.leaguename} = 'Big'"

    M, q, _ = athletes_in_big_league()
    assert q == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team FROM Athlete
        JOIN Team team ON Athlete.team = team.id
        JOIN League team_league ON team.league = team_league.id
        WHERE team_league.leaguename = 'Big'
        """)


def test_select_with_parameters() -> None:
    @select(Athlete)
    def athletes_in_league(league: str):
        return f"WHERE {Athlete.team.league.leaguename} = {league}"

    M, q, p = athletes_in_league('Big')
    assert q == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team FROM Athlete
        JOIN Team team ON Athlete.team = team.id
        JOIN League team_league ON team.league = team_league.id
        WHERE team_league.leaguename = :league
        """)

    assert p == {'league': 'Big'}


def test_select_decorator_runs_eagerly() -> None:
    with pytest.raises(QueryError, match="must be either Fields of Models or parameters"):

        @select(Athlete)
        def malformed():
            return f"WHERE {8888} = 1"


# Error cases


def test_select_with_unused_parameter() -> None:
    with pytest.raises(QueryError, match="Unused parameter"):

        @select(Athlete)
        def athletes_in_league(league: str, unused: str):
            return f"WHERE {Athlete.team.league.leaguename} = {league}"


# TODO: More edges
# test_select_with_field_root_unmatched_with_model
