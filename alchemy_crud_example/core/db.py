from sqlalchemy import create_engine, select, insert, update, delete
from sqlalchemy.sql import text
from datetime import datetime

from alchemy_crud_example.servises import clear_db, split_into_tables


class DataBase:
    def __init__(self, database_name):
        self.engine = create_engine(
            f'sqlite:///{database_name}',
            echo=False
        )

        self.engine.connect()

        from alchemy_crud_example.core.models import (
            metadata, customers, orders, order_lines, items
        )

        self.metadata = metadata
        self.metadata.create_all(self.engine)

        self.customers = customers
        self.orders = orders
        self.order_lines = order_lines
        self.items = items

        print('core database is init')

    def clear_db(self):
        clear_db(self.engine)

    def _id_search_with_insert(self, table, row, row_at_creating=None):
        table_name = str(table.name)
        where = "AND ".join(
            f'{table_name}.{k} = "{row[k]}" '
            for k in row
        )

        s = text(f'SELECT {table_name}.id FROM {table_name} WHERE {where} ')

        with self.engine.connect() as conn:
            rs = conn.execute(s)

        row_id = rs.first()

        if not row_id:

            if row_at_creating:
                row.update(row_at_creating)

            s = insert(table).values(row)

            with self.engine.connect() as conn:
                rs = conn.execute(s)
                conn.commit()

            row_id = rs.inserted_primary_key

        return row_id[0]

    def _purchase_without_items_and_customer(self, purchase):
        customer_row, orders_row, \
            order_lines_row, items_row = split_into_tables(purchase)

        items_id = self._id_search_with_insert(self.items, items_row)

        order_lines_row['item_id'] = items_id

        customer_id = self._id_search_with_insert(self.customers, customer_row)

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
        s = (
            select(
                self.customers.c.first_name,
                self.customers.c.last_name,
                self.customers.c.username,
                self.customers.c.email,
                self.customers.c.address,
                self.customers.c.town,
                self.items.c.name.label('item'),
                self.items.c.cost_price,
                self.items.c.selling_price,
                self.orders.c.number,
                self.order_lines.c.id.label('purchase_id'),
                self.order_lines.c.quantity)
            .join_from(self.orders, self.order_lines)
            .join_from(self.orders, self.customers)
            .join_from(self.order_lines, self.items).
            where(self.orders.c.number == int(order_number))
        )

        rows = []
        with self.engine.connect() as conn:
            for row in conn.execute(s):
                rows.append(row._mapping)

        return rows

    def delete(self, order_number):
        s = select(self.orders.c.id) \
            .where(self.orders.c.number == int(order_number))

        with self.engine.connect() as conn:
            rs = conn.execute(s)

        order_id = rs.first()[0]

        s_order_lines = delete(self.order_lines) \
            .where(self.order_lines.c.order_id == int(order_id))

        s_orders = delete(self.orders) \
            .where(self.orders.c.id == int(order_id))

        with self.engine.connect() as conn:
            conn.execute(s_order_lines)
            conn.execute(s_orders)
            conn.commit()

    def update(self, new_purchase):
        order_lines_row, orders_row, \
            date_for_orders_row = self._purchase_without_items_and_customer(new_purchase)

        s = select(self.order_lines.c.order_id) \
            .where(self.order_lines.c.id == int(new_purchase["purchase_id"]))

        with self.engine.connect() as conn:
            rs = conn.execute(s)

        order_id = rs.first()[0]

        order_lines_row['order_id'] = order_id

        s_orders = update(self.orders).values(orders_row) \
            .where(self.orders.c.id == int(order_id))

        s_order_lines = update(self.order_lines).values(order_lines_row) \
            .where(self.order_lines.c.id == int(new_purchase["purchase_id"]))

        with self.engine.connect() as conn:
            conn.execute(s_orders)
            conn.execute(s_order_lines)
            conn.commit()
