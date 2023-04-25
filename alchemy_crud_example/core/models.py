from datetime import datetime

from sqlalchemy import (MetaData, Table, Column, ForeignKey,
                        Integer, String, DateTime, Numeric)


metadata = MetaData()


customers = Table('customers', metadata,
                  Column('id', Integer(), primary_key=True),
                  Column('first_name', String(100), nullable=False),
                  Column('last_name', String(100), nullable=False),
                  Column('username', String(50), nullable=False),
                  Column('email', String(200), nullable=False),
                  Column('address', String(200), nullable=False),
                  Column('town', String(50), nullable=False)
                  )

items = Table('items', metadata,
              Column('id', Integer(), primary_key=True),
              Column('name', String(200), nullable=False),
              Column('cost_price', Numeric(10, 2), nullable=False),
              Column('selling_price', Numeric(10, 2), nullable=False),
              )

orders = Table('orders', metadata,
               Column('id', Integer(), primary_key=True),
               Column('number', Integer(), nullable=False),
               Column('customer_id', ForeignKey('customers.id')),
               Column('date_placed', DateTime(), default=datetime.now)
               )

order_lines = Table('order_lines', metadata,
                    Column('id', Integer(), primary_key=True),
                    Column('order_id', ForeignKey('orders.id')),
                    Column('item_id', ForeignKey('items.id')),
                    Column('quantity', Integer())
                    )


print('core models is init')
