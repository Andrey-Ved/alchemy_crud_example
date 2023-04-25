from random import choice


def split_into_tables(purchase):
    customer_row = {
        'first_name': purchase['first_name'],
        'last_name': purchase['last_name'],
        'username': purchase['username'],
        'email': purchase['email'],
        'address': purchase['address'],
        'town': purchase['town']
    }

    items_row = {
        'name': purchase['item'],
        'cost_price': purchase['cost_price'],
        'selling_price': purchase['selling_price']
    }

    orders_row = {
        'number': purchase['order'],
    }

    order_lines_row = {
        'id': purchase['purchase_id'],
        'quantity': purchase['quantity']
    }

    return customer_row, orders_row, order_lines_row, items_row


def random_order(data):
    orders = set()

    for purchase in data:
        orders.add(purchase['order'])

    return choice(list(orders))


def print_purchase(repository, order_number):
    purchases_list = repository.read(order_number)
    if purchases_list:
        for purchase in purchases_list:
            print(purchase)
    else:
        print(f'no data with order number {order_number}')


def _random_purchase(data, order_number):
    purchases = []

    for purchase in data:
        if purchase['order'] == order_number:
            purchases.append(purchase)

    return choice(purchases)


def random_update(data, order_number):
    purchase = _random_purchase(data, order_number)

    new_purchase = purchase.copy()
    new_purchase["first_name"] = 'Update'
    new_purchase["item"] = 'Update'

    return new_purchase
