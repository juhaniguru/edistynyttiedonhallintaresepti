import contextlib
import datetime
import multiprocessing

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


@contextlib.contextmanager
def get_db():
    # engine = None
    db = None
    try:
        print("############ in try ##########################")
        engine = create_engine('mysql+mysqlconnector://reseptit2:salasana@localhost/reseptit')
        db_session = sessionmaker(bind=engine)
        db = db_session()
        yield db
    except Exception as e:
        print(e)
    finally:
        print("############### in finally ############")
        if db is not None:
            db.close()


def insert_category():
    with get_db() as _db:
        query = "INSERT INTO categories(name) VALUES(:category_name)"
        statement = text(query)

        _db.execute(statement, {'category_name': 'Juustot4'})
        _db.commit()
        print("##### commited")

    print("done")


def insert_many_batches(num_of_iterations=10):
    for i in range(num_of_iterations):
        insert_categories_batch(iteration=i, number_of_rows=10000)


def insert_categories_batch(iteration=1, number_of_rows=1000):
    start = datetime.datetime.now()
    with get_db() as _db:
        query = "INSERT INTO categories(name) VALUES"
        variables = {}
        for i in range(number_of_rows):
            query = query + f"(:category_name{i}),"
            variables[f'category_name{i}'] = f'Kategoria{iteration}{i}'
        query = query[:-1]
        statement = text(query)
        _db.execute(statement, variables)
        _db.commit()
        end = datetime.datetime.now()
        print("###### aikaa kulunut:", end - start)


def insert_categories(number_of_rows=1000):
    start = datetime.datetime.now()
    with get_db() as _db:
        query = "INSERT INTO categories(name) VALUES(:category_name)"
        for i in range(number_of_rows):
            statement = text(query)
            _db.execute(statement, {'category_name': f'Kategoria{i}'})
        _db.commit()
    end = datetime.datetime.now()
    print("######### aikaa kulunut:", end - start)


if __name__ == '__main__':
    start = datetime.datetime.now()
    processes = []
    for i in range(10):
        p = multiprocessing.Process(target=insert_categories_batch, args=(i, 10000))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    end = datetime.datetime.now()
    print("######## kaikki prosessit valmiita", end - start)
