import random
import uuid
from collections import defaultdict

import pandas as pd


def generate():
    # Number of events to generate
    num_events = 10_000

    # Generate Event Time
    event_time = pd.date_range(start="2021-01-01", end="2021-12-31", freq="H")
    event_time = random.choices(event_time, k=num_events)
    event_time.sort()

    # Generate Event ID
    event_ids = [str(uuid.uuid4()) for _ in range(num_events)]

    # Generate Customers and User IDs
    customers = ["Company_" + str(i) for i in range(100)]
    user_ids = ["User_" + str(i) for i in range(1000)]

    mapping = defaultdict(list)
    for user_id in user_ids:
        customer = random.choice(customers)
        mapping[customer].append(user_id)

    customer_column = []
    user_id_column = []

    for _ in range(num_events):
        customer = random.choice(customers)

        if not mapping[customer]:
            continue

        user_id = random.choice(mapping[customer])

        customer_column.append(customer)
        user_id_column.append(user_id)

    # Create DataFrame
    df_unique_users_fixed = pd.DataFrame(
        {
            "event_time": event_time,
            "event_id": event_ids,
            "customer": customer_column,
            "user_id": user_id_column,
        }
    )

    df_unique_users_fixed.to_csv("event_data.csv", index=False)


if __name__ == "__main__":
    generate()
