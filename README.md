# asyncpg type issue

In using `asyncpg` I've run into a couple of issues when trying to run queries which otherwise work in `psycopg2` and `psql`.

There are test cases for them in `tests`. To reproduce:

1. `createdb asyncpg-type-issue`
2. `poetry install` or `pip install -r requirements.txt`
3. `poetry run pytest`
