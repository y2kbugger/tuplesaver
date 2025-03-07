from textwrap import dedent
from typing import NamedTuple

import pytest

from .query2 import QueryError, select


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


def dd(sql: str) -> str:
    return dedent(sql).strip()


def test_select_on_table() -> None:
    M, q = select(Athlete)
    assert q == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team FROM Athlete
        """)


def test_select_on_table_with_where() -> None:
    @select(Athlete)
    def big_leagues():
        return f"WHERE {Athlete.name} = 'Joe'"

    M, q = big_leagues()
    assert q == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team FROM Athlete
        WHERE Athlete.name = 'Joe'
        """)


def test_select_on_table_with_join_caused_by_predicate() -> None:
    @select(Athlete)
    def athletes_on_red_team():
        return f"WHERE {Athlete.team.teamname} = 'Red Snickers'"

    M, q = athletes_on_red_team()
    assert q == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team FROM Athlete
        JOIN Team team ON Athlete.team = team.id
        WHERE team.teamname = 'Red Snickers'
        """)


def test_select_on_table_with_multiple_implicit_joins() -> None:
    @select(Athlete)
    def athletes_in_big_league():
        return f"WHERE {Athlete.team.league.leaguename} = 'Big'"

    M, q = athletes_in_big_league()
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

    M, q = athletes_in_league()
    assert q == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team FROM Athlete
        JOIN Team team ON Athlete.team = team.id
        JOIN League team_league ON team.league = team_league.id
        WHERE team_league.leaguename = :league
        """)


# Error cases


def test_select_with_unused_parameter() -> None:
    with pytest.raises(QueryError, match="Unused parameter"):

        @select(Athlete)
        def athletes_in_league(league: str, unused: str):
            return f"WHERE {Athlete.team.league.leaguename} = {league}"


# TODO: More edges
# test_select_with_field_root_unmatched_with_model
