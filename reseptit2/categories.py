import datetime
import multiprocessing
import uuid

from sqlalchemy import text
from tools import get_db


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


def insert_categories_batch_mp():
    start = datetime.datetime.now()
    processes = []
    for i in range(10):
        pr = multiprocessing.Process(target=insert_categories_batch, args=(10000,))
        pr.start()
        processes.append(pr)

    for pr in processes:
        pr.join()

    end = datetime.datetime.now()
    print("####### aika kulunut kun kaikki valmiita:", end - start)


def insert_categories_many_batches(number_of_batches=10):
    start = datetime.datetime.now()
    for i in range(number_of_batches):
        insert_categories_batch()

    end = datetime.datetime.now()
    print("##### aikaa kulunut:", end - start)


def insert_categories_batch(number_of_categories=10000):
    start = datetime.datetime.now()
    with get_db() as _db:
        query = "INSERT INTO categories(name) VALUES"
        query_variables = {}
        for i in range(number_of_categories):
            _random_str = str(uuid.uuid4())
            query = query + f"(:category_name{i}),"
            query_variables[f'category_name{i}'] = f'Kategoria{_random_str}'

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


multiprocessing.poo

if __name__ == '__main__':
    insert_categories_batch_mp()
