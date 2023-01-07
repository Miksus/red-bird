import pytest

@pytest.fixture()
def engine():
    sqlalchemy = pytest.importorskip("sqlalchemy")
    engine = sqlalchemy.create_engine('sqlite://')
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("""
        CREATE TABLE empty (
            id TEXT PRIMARY KEY,
            name TEXT,
            birth_date DATE,
            score INTEGER
        )
        """))

        conn.execute(sqlalchemy.text("""
        CREATE TABLE populated (
            id TEXT PRIMARY KEY,
            name TEXT,
            birth_date DATE,
            score INTEGER
        )
        """))
        conn.execute(sqlalchemy.text("""
        INSERT INTO populated (id, name, birth_date, score)
        VALUES ('a', 'Jack', '2000-01-01', 100),
        ('b', 'John', '1990-01-01', 200),
        ('c', 'James', '2020-01-01', 300)
        """))
    return engine