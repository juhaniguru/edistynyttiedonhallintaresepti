import contextlib
import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, \
    close_all_sessions


@contextlib.contextmanager
def get_db():
    engine = None
    db_session = None
    try:
        print("##### in get_db try block")
        engine = create_engine('mysql+mysqlconnector://root:@localhost/reseptit')
        db_session = sessionmaker(bind=engine)
        db = db_session()
        yield db
    except Exception as e:
        print(e)
    finally:
        print("##### in get_db finally block")
        if db_session is not None:
            close_all_sessions()
        if engine is not None:
            engine.dispose()


def insert_categories_with_1_commit(number_of_categories=1000):
    start = datetime.datetime.now()
    with get_db() as _db:
        for i in range(number_of_categories):
            query = "INSERT INTO categories(name) VALUES(:category_name)"
            statement = text(query)
            _db.execute(statement, {'category_name': f'Kategoria{i + 1}'})
        _db.commit()
    end = datetime.datetime.now()
    print("#### elapsed time in insert_categories:", end - start)


def insert_categories(number_of_categories=1000):
    start = datetime.datetime.now()
    with get_db() as _db:
        for i in range(number_of_categories):
            query = "INSERT INTO categories(name) VALUES(:category_name)"
            statement = text(query)
            _db.execute(statement, {'category_name': f'Kategoria{i + 1}'})
            _db.commit()
    end = datetime.datetime.now()
    print("#### elapsed time in insert_categories:", end - start)


def insert_categories_batch(number_of_categories=10000):
    start = datetime.datetime.now()
    with get_db() as _db:
        query = "INSERT INTO categories(name) VALUES"
        query_variables = {}
        for i in range(number_of_categories):
            query = query + f"(:category_name{i}),"
            query_variables[f'category_name{i}'] = f'Kategoria{i+1}'

        query = query[:-1]
        _db.execute(text(query), query_variables)
        _db.commit()
    end = datetime.datetime.now()
    print("##### elapsed time:", end - start)


def insert_category():
    with get_db() as _db:
        print("### inside get_db context")
        query = "INSERT INTO categories(name) VALUES(:category_name)"
        statement = text(query)
        _db.execute(statement, {'category_name': 'Salaatit'})
        _db.commit()
    print("#### in insert_categories after get_db")


insert_categories_batch()
