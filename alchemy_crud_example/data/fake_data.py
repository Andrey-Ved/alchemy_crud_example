from alchemy_crud_example.data.data import customers_list, items_list
from random import randint, shuffle

def fake_data():
    data_list = []
    orders_count = 0
    purchase_count = 0
    for id, customer in enumerate(customers_list):
        orders_count += 1
        shuffle(items_list)
        orders1_lines_number = randint(1, 2)
        for i in range(orders1_lines_number):
            purchase_count += 1
            row = customer.copy()
            row['id'] = id
            row['order'] = orders_count
            row["item"] = items_list[i]["name"]
            row["cost_price"] = items_list[i]["cost_price"]
            row["selling_price"] = items_list[i]["selling_price"]
            row["quantity"] = randint(1, 20)
            row["purchase_id"] = purchase_count
            data_list.append(row)

        orders_count += 1
        shuffle(items_list)
        orders2_lines_number = randint(0, 3)
        for i in range(orders2_lines_number):
            purchase_count += 1
            row = customer.copy()
            row['id'] = id
            row['order'] = orders_count
            row["item"] = items_list[i]["name"]
            row["cost_price"] = items_list[i]["cost_price"]
            row["selling_price"] = items_list[i]["selling_price"]
            row["quantity"] = randint(1, 20)
            row["purchase_id"] = purchase_count
            data_list.append(row)

        orders_count += 1
        shuffle(items_list)
        orders3_lines_number = randint(0, 2)
        for i in range(orders3_lines_number):
            purchase_count += 1
            row = customer.copy()
            row['id'] = id
            row['order'] = orders_count
            row["item"] = items_list[i]["name"]
            row["cost_price"] = items_list[i]["cost_price"]
            row["selling_price"] = items_list[i]["selling_price"]
            row["quantity"] = randint(1, 20)
            row["purchase_id"] = purchase_count
            data_list.append(row)

    return data_list

if __name__ == '__main__':
    data_list = fake_data()

    for i in data_list:
        print(i)
