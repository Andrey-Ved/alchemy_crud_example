from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

from alchemy_crud_example.core.db import DataBase as CoreDataBase


class DataBase(CoreDataBase):
    def __init__(self, database_name):
        self.engine = create_engine(
            f'sqlite:///{database_name}',
            echo=False
        )

        from alchemy_crud_example.orm.models import (
            Base, Customer, Order, OrderLine, Item
        )

        self.Base = Base
        self.Base.metadata.create_all(self.engine)

        self.Session = sessionmaker(bind=self.engine)

        self.customers = Customer
        self.orders = Order
        self.order_lines = OrderLine
        self.items = Item

        print('orm database is init')

    def _id_search_with_insert(self, model, row, row_at_creating=None):
        with self.Session() as session:
            where = and_(*[getattr(model, k) == row[k] for k in row])
            row_id = session.query(model.id).filter(where).first()

        if row_id:
            return row_id[0]
        else:
            if row_at_creating:
                row.update(row_at_creating)

            with self.Session() as session:
                with session.begin():
                    s = model(**row)
                    session.add(s)
                row_id = str(s.id)

            return row_id

    def create(self, purchase):
        (order_lines_row,
         orders_row,
         date_for_orders_row) = self._purchase_without_items_and_customer(purchase)

        orders_id = self._id_search_with_insert(
            self.orders,
            orders_row,
            date_for_orders_row
        )

        order_lines_row['order_id'] = orders_id

        order_lines_id = self._id_search_with_insert(
            self.order_lines,
            order_lines_row
        )

        return order_lines_id

    def read(self, order_number):
        rows = []
        with self.Session() as session:
            rs = session.query(
                self.customers.first_name,
                self.customers.last_name,
                self.customers.username,
                self.customers.email,
                self.customers.address,
                self.customers.town,
                self.items.name.label('item'),
                self.items.cost_price,
                self.items.selling_price,
                self.orders.number,
                self.order_lines.id.label('purchase_id'),
                self.order_lines.quantity
            ) \
                .select_from(self.customers) \
                .join(self.orders) \
                .join(self.order_lines) \
                .join(self.items) \
                .filter(self.orders.number == int(order_number)) \
                .all()

        for row in rs:
            rows.append(row._mapping)

        return rows

    def delete(self, order_number):
        with self.Session() as session:
            with session.begin():
                rs = session.query(self.orders.id). \
                    filter(self.orders.number == int(order_number)).first()

                order_id = rs[0]

                session.query(self.order_lines) \
                    .where(self.order_lines.order_id == int(order_id)) \
                    .delete(synchronize_session=False)

                session.query(self.orders) \
                    .where(self.orders.id == int(order_id)) \
                    .delete(synchronize_session=False)

    def update(self, new_purchase):
        order_lines_row, orders_row, \
            date_for_orders_row = self._purchase_without_items_and_customer(new_purchase)

        with self.Session() as session:
            with session.begin():
                rs = session.query(self.order_lines.order_id). \
                    filter(self.order_lines.id == int(new_purchase["purchase_id"])).first()

                order_id = rs[0]

                order_lines_row['order_id'] = order_id

                session.query(self.orders) \
                    .filter(self.orders.id == int(order_id)) \
                    .update(orders_row, synchronize_session=False)

                session.query(self.order_lines) \
                    .filter(self.order_lines.id == int(new_purchase["purchase_id"])) \
                    .update(order_lines_row, synchronize_session=False)
