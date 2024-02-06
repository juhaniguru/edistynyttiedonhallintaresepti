import random

from faker import Faker
from faker_food import FoodProvider
from sqlalchemy import text

from tools import get_db


def _get_categories(_db):
    query = "SELECT id FROM categories"
    ids = []
    statement = text(query)
    rows = _db.execute(statement)
    categories = rows.all()
    for category in categories:
        ids.append(category[0])
    return ids


def insert_recipes():
    fake = Faker()
    fake.add_provider(FoodProvider)
    with get_db() as _db:
        categories = _get_categories(_db)
        #users = _get_users(_db)
        random_category_index = random.randint(0, len(categories)-1)
        random_category_id = categories[random_category_index]
        print("random", random_category_id)
        query = "INSERT INTO recipe()"


def insert_ingredients():
    fake = Faker()
    fake.add_provider(FoodProvider)
    with get_db() as _db:
        query = "INSERT INTO ingredients(ingredient) VALUES(:ingredient)"

        for i in range(100):
            try:
                statement = text(query)
                _db.execute(statement, {'ingredient': fake.ingredient()})
                _db.commit()
            except Exception as e:
                print(e)


while True:
    choice = input("Lisää asioita: (q=lopeta, 1=ingredients, 2=recipes: ")
    if choice == 'q':
        break

    elif choice == '1':
        insert_ingredients()
    elif choice == '2':
        insert_recipes()
