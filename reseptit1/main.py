import contextlib

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, close_all_sessions


@contextlib.contextmanager
def get_db():
    engine = None
    db = None
    try:
        print("############ in try ##########################")
        engine = create_engine('mysql+mysqlconnector://root:@localhost/reseptit')
        db_session = sessionmaker(bind=engine)
        db = db_session()
        yield db
    except Exception as e:
        print(e)
    finally:
        print("############### in finally ############")
        if db is not None:
            close_all_sessions()
        if engine is not None:
            engine.dispose()


def insert_categories():
    with get_db() as _db:
        query = "INSERT INTO categories(name) VALUES(:category_name)"
        statement = text(query)

        _db.execute(statement, {'category_name': 'Juustot'})
        _db.commit()

    print("done")


if __name__ == '__main__':
    insert_categories()
