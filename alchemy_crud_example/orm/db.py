from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from alchemy_crud_example.servises import clear_db, split_into_tables


class DataBase:
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

        self.Customer = Customer
        self.Order = Order
        self.OrderLine = OrderLine
        self.Item = Item

        print('orm database is init')

    def clear_db(self):
        clear_db(self.engine)

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

    def _purchase_without_items_and_customer(self, purchase):
        customer_row, orders_row, \
            order_lines_row, items_row = split_into_tables(purchase)

        items_id = self._id_search_with_insert(self.Item, items_row)

        order_lines_row['item_id'] = items_id

        customer_id = self._id_search_with_insert(self.Customer, customer_row)

        orders_row['customer_id'] = customer_id
        orders_row['date_placed'] = datetime.now()
        date_for_orders_row = {'date_placed': orders_row.pop('date_placed')}

        return (order_lines_row,
                orders_row,
                date_for_orders_row)

    def create(self, purchase):
        (order_lines_row,
         orders_row,
         date_for_orders_row) = self._purchase_without_items_and_customer(purchase)

        orders_id = self._id_search_with_insert(
            self.Order,
            orders_row,
            date_for_orders_row
        )

        order_lines_row['order_id'] = orders_id

        order_lines_id = self._id_search_with_insert(
            self.OrderLine,
            order_lines_row
        )

        return order_lines_id

    def read(self, order_number):
        rows = []
        with self.Session() as session:
            rs = session.query(
                self.Customer.first_name,
                self.Customer.last_name,
                self.Customer.username,
                self.Customer.email,
                self.Customer.address,
                self.Customer.town,
                self.Item.name.label('item'),
                self.Item.cost_price,
                self.Item.selling_price,
                self.Order.number,
                self.OrderLine.id.label('purchase_id'),
                self.OrderLine.quantity
            ) \
                .select_from(self.Customer) \
                .join(self.Order) \
                .join(self.OrderLine) \
                .join(self.Item) \
                .filter(self.Order.number == int(order_number)) \
                .all()

        for row in rs:
            rows.append(row._mapping)

        return rows

    def delete(self, order_number):
        with self.Session() as session:
            with session.begin():
                rs = session.query(self.Order.id). \
                    filter(self.Order.number == int(order_number)).first()

                order_id = rs[0]

                session.query(self.OrderLine) \
                    .where(self.OrderLine.order_id == int(order_id))\
                    .delete(synchronize_session=False)

                session.query(self.Order) \
                    .where(self.Order.id == int(order_id))\
                    .delete(synchronize_session=False)

    def update(self, new_purchase):
        order_lines_row, orders_row, \
            date_for_orders_row = self._purchase_without_items_and_customer(new_purchase)

        with self.Session() as session:
            with session.begin():
                rs = session.query(self.OrderLine.order_id). \
                    filter(self.OrderLine.id == int(new_purchase["purchase_id"])).first()

                order_id = rs[0]

                order_lines_row['order_id'] = order_id

                session.query(self.Order)\
                    .filter(self.Order.id == int(order_id))\
                    .update(orders_row, synchronize_session=False)

                session.query(self.OrderLine)\
                    .filter(self.OrderLine.id == int(new_purchase["purchase_id"]))\
                    .update(order_lines_row, synchronize_session=False)
