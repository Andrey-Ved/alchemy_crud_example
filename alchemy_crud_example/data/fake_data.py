from alchemy_crud_example.data.data import customers_list, items_list
from random import randint, shuffle


def fake_data():
    data_list = []
    orders_count = 0
    purchase_count = 0

    for id, customer in enumerate(customers_list):
        for borders in (1, 2), (0, 3), (0,2):
            orders_count += 1

            shuffle(items_list)
            orders_lines_number = randint(*borders)

            for item in range(orders_lines_number):
                purchase_count += 1

                row = customer.copy()
                row['id'] = id
                row['order'] = orders_count
                row["item"] = items_list[item]["name"]
                row["cost_price"] = items_list[item]["cost_price"]
                row["selling_price"] = items_list[item]["selling_price"]
                row["quantity"] = randint(1, 20)
                row["purchase_id"] = purchase_count

                data_list.append(row)

    return data_list


def _show_fake_data():
    data_list = fake_data()

    for i in data_list:
        print(i)


if __name__ == '__main__':
    _show_fake_data()