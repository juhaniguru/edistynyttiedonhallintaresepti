from faker import Faker
from sqlalchemy import text

from tools import get_db


def insert_users(number_of_users=1000):
    with get_db() as _db:
        fake = Faker()
        query = "INSERT INTO users(username, password, auth_role_id) VALUES"
        variable_values = {}

        for i in range(number_of_users):
            variable_values[f'username{i}'] = fake.name()
            variable_values[f'password{i}'] = f'lsfdkjsfklsfdj{i}'
            variable_values[f'role{i}'] = 1
            query += f'(:username{i}, :password{i}, :role{i}),'
        # poistataan viimeinen pilkku
        query = query[:-1]
        statement = text(query)
        _db.execute(statement, variable_values)
        _db.commit()


insert_users(number_of_users=10)
