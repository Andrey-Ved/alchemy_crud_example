from alchemy_crud_example.repository import Repository
from alchemy_crud_example.data.fake_data import fake_data

from alchemy_crud_example.data.servises import (
    random_order, random_update, print_purchase
)


def main():
    data = fake_data()

    repository = Repository()

    repository.clear_db()
    print(f'\n+++ start')

    order_number = random_order(data)
    print(f'\n+++ random order number - {order_number}')

    print(f'\n+++ unfilled version')
    print_purchase(repository, order_number)

    for purchase in data:
        repository.create(purchase)
    print(f'\n+++ filled version')
    print_purchase(repository, order_number)

    new_purchase = random_update(data, order_number)
    repository.update(new_purchase)
    print(f'\n+++ updated version')
    print_purchase(repository, order_number)

    repository.delete(order_number)
    print(f'\n+++ deleted version')
    print_purchase(repository, order_number)


if __name__ == '__main__':
    main()
