from textwrap import dedent

import apsw

from sqlir.engine import Engine
from sqlir.lazy import Rows
from sqlir.model import Row, TableRow, backref
from sqlir.sql import build_select_sql


class League(TableRow):
    leaguename: str


class Athlete(TableRow):
    name: str
    team: Team
    number: int


class Team(TableRow):
    teamname: str
    league: League
    athletes: Rows[Athlete] = backref(fk=Athlete.team)


class AthleteView(Row):
    __tablename__ = "Athlete"
    name: str


def dd(sql: str) -> str:
    return dedent(sql).strip()


def test_select_on_table() -> None:
    engine = Engine(apsw.Connection(":memory:"))
    engine.ensure_table_created(League)
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Athlete)

    q = engine.select(Athlete)

    # Check that without params, it's just selecting the table
    # Since we can't easily introspect the query from engine.select's public API without executing,
    # we'll just check that it runs
    assert q.fetchall() == []


def test_select_on_view_model() -> None:
    engine = Engine(apsw.Connection(":memory:"))
    engine.ensure_table_created(Athlete)

    # We create a dummy view and select from it
    # Currently engine.select needs TableRow or Row with __tablename__
    query = engine.select(AthleteView)
    assert query.fetchall() == []


def test_select_where_exists_clause_starts_on_own_line() -> None:
    params: dict[str, object] = {}

    sql = build_select_sql(Athlete, Athlete.team.teamname == "Red", params)

    assert sql == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team, Athlete.number FROM Athlete
        WHERE EXISTS (
            SELECT 1 FROM Team team
            WHERE team.id = Athlete.team
            AND team.teamname = :p0
        )
    """)
    assert params == {"p0": "Red"}


def test_select_where_simple_clause_starts_on_own_line() -> None:
    params: dict[str, object] = {}

    sql = build_select_sql(Athlete, 1, params, limit=1)

    assert sql == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team, Athlete.number FROM Athlete
        WHERE id = :p0
        LIMIT 1
    """)
    assert params == {"p0": 1}


def test_select_where_tstring_scalar_subquery_clause_starts_on_own_line() -> None:
    params: dict[str, object] = {}

    sql = build_select_sql(Athlete, t"{Athlete.team.league.leaguename} LIKE 'B%'", params)

    assert sql == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team, Athlete.number FROM Athlete
        WHERE (
            SELECT (
                SELECT team_league.leaguename
                FROM League team_league
                WHERE team_league.id = team.league
            )
            FROM Team team
            WHERE team.id = Athlete.team
        ) LIKE 'B%'
    """)
    assert params == {}


def test_select_order_by_tstring_field() -> None:
    params: dict[str, object] = {}

    sql = build_select_sql(Athlete, None, params, order=t"{Athlete.number} DESC")

    assert sql == "SELECT Athlete.id, Athlete.name, Athlete.team, Athlete.number FROM Athlete ORDER BY Athlete.number DESC"
    assert params == {}


def test_select_order_by_tstring_fk_path_uses_scalar_subquery() -> None:
    params: dict[str, object] = {}

    sql = build_select_sql(Athlete, None, params, order=t"{Athlete.team.teamname}")

    assert sql == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team, Athlete.number FROM Athlete ORDER BY (
            SELECT team.teamname
            FROM Team team
            WHERE team.id = Athlete.team
        )
    """)
    assert params == {}


def test_select_order_by_tstring_with_where_target() -> None:
    params: dict[str, object] = {}

    sql = build_select_sql(Athlete, Athlete.number > 5, params, order=t"{Athlete.name} ASC", limit=2)

    assert sql == dd("""
        SELECT Athlete.id, Athlete.name, Athlete.team, Athlete.number FROM Athlete
        WHERE Athlete.number > :p0
        ORDER BY Athlete.name ASC
        LIMIT 2
    """)
    assert params == {"p0": 5}


def test_fanout_not_occur() -> None:
    engine = Engine(apsw.Connection(":memory:"))
    engine.ensure_table_created(League)
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Athlete)

    # Insert a situation where a JOIN might cause a fanout if we selected the ONE side based on the MANY side.
    # Here Athlete -> Team is Many to One.
    # To test fanout from standard paths, let's just make sure queries spanning relationships don't return duplicates
    # of the base object by incorrectly joining.
    league = engine.insert(League(leaguename="Big"))

    team_red = engine.insert(Team(teamname="Red", league=league))
    team_blue = engine.insert(Team(teamname="Blue", league=league))
    team_yellow = engine.insert(Team(teamname="Yellow", league=league))

    players = [
        Athlete("Alice", team_red, 1),
        Athlete("Bob", team_red, 2),
        Athlete("Charlie", team_red, 3),
        Athlete("Xanadu", team_blue, 7),  # 1
        Athlete("Yvonne", team_blue, 7),  # 2, two players from this team
        Athlete("Zak", team_blue, 9),
        Athlete("Melinda", team_yellow, 7),  # 3
    ]
    for player in players:
        engine.insert(player)

    # normal usage to check condition on related table
    cur_semi = engine.select(Team, Team.athletes[0].number == 7)
    # assert it generates the EXISTS implicitly via normal usage
    assert "EXISTS" in cur_semi.sql

    rows_semi = cur_semi.fetchall()

    # only two teams have a player with #7 (Blue has two, Yellow one), even
    # though there are three players #7 total — the semi-join must not fan out.
    assert len(rows_semi) == 2
    assert {r.teamname for r in rows_semi} == {"Blue", "Yellow"}

    # A join from Team (one side) to Athlete (many side) fans out. Bind the raw
    # join to a `Row` model via `__select_query__` instead of running raw SQL.
    class TeamFanout(Row):
        teamname: str

        __select_query__ = t"""
            SELECT {Team.teamname} FROM {Team}
            JOIN {Athlete} ON {Athlete.team} = {Team.id}
            WHERE {Athlete.number} = 7
            """

    rows_join = engine.select(TeamFanout).fetchall()
    assert len(rows_join) == 3
    assert [r.teamname for r in rows_join] == ["Blue", "Blue", "Yellow"]
