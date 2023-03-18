import pytest
import asyncpg
import psycopg2

TABLE = """
CREATE TABLE IF NOT EXISTS asyncpg_issue (
    id SERIAL PRIMARY KEY
    , date DATE
);
"""

DSN = "postgresql://localhost/asyncpg-type-issue"

@pytest.mark.asyncio
async def test_asyncpg_integer_cast():
    """
    asyncpg.exceptions.DataError: invalid input for query argument $1:
    '1' ('str' object cannot be interpreted as an integer)
    """
    conn = await asyncpg.connect(DSN)
    await conn.execute(TABLE)
    await conn.execute(
        "select id, date from asyncpg_issue where id = $1::integer",
        "1"
    )
    await conn.close()

@pytest.mark.asyncio
async def test_asyncpg_interval():
    """
    asyncpg.exceptions.PostgresSyntaxError: syntax error at or near "$1"
    """
    conn = await asyncpg.connect(DSN)
    await conn.execute(TABLE)
    await conn.execute(
        "select id, date from asyncpg_issue where date > now() + interval $1",
        '2 weeks ago'
    )
    conn.close()

@pytest.mark.asyncio
async def test_asyncpg_interval_cast():
    """
    syncpg.exceptions.DataError: invalid input for query argument $1:
    '14 day' ('str' object has no attribute 'days')
    """
    conn = await asyncpg.connect(DSN)
    await conn.execute(TABLE)
    await conn.execute(
        "select id, date from asyncpg_issue where date > now() + $1::interval",
        '14 day'
    )
    conn.close()

def test_psycopg_integer_cast():
    conn = psycopg2.connect(dsn=DSN)
    cur = conn.cursor()

    cur.execute(TABLE)
    cur.execute("select id, date from asyncpg_issue where id = (%s)::integer", ("1"))

    cur.close()
    conn.close()

def test_psycopg_interval():
    conn = psycopg2.connect(dsn=DSN)
    cur = conn.cursor()

    cur.execute(TABLE)
    cur.execute(
        "select id, date from asyncpg_issue where date > now() + interval %s",
        ("2 weeks ago",)
    )

    cur.close()
    conn.close()
    
def test_psycopg_interval_cast():
    conn = psycopg2.connect(dsn=DSN)
    cur = conn.cursor()

    cur.execute(TABLE)
    cur.execute(
        "select id, date from asyncpg_issue where date > now() + %s::interval",
        ("14 day",)
    )

    cur.close()
    conn.close()