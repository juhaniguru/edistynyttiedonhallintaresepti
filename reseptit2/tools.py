import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@contextlib.contextmanager
def get_db():
    db = None
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
        db.close()