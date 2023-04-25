from alchemy_crud_example.core.db import DataBase as CoreDataBase
from alchemy_crud_example.orm.db import DataBase as ORMDataBase
from alchemy_crud_example.config import DATABASE_NAME, ORM_USE


if ORM_USE:
    database = ORMDataBase(DATABASE_NAME)
else:
    database = CoreDataBase(DATABASE_NAME)


class Repository:
    def __init__(self, work_db=database):
        self.database = work_db
        # self.engine = self.database.engine
        print('Repository is init')

    def clear_db(self):
        self.database.clear_db()

    def create(self, purchase):
        return self.database.create(purchase)

    def read(self, order_id):
        return self.database.read(order_id)

    def delete(self, order_id):
        return self.database.delete(order_id)

    def update(self, new_purchase):
        return self.database.update(new_purchase)
