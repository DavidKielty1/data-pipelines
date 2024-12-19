import pandas as pd
from random import randint, choice
from datetime import datetime, timedelta

# Generate mock user logs
users = [
    {
        "user_id": i,
        "search_date": (datetime.now() - timedelta(days=randint(1, 30))).strftime("%Y-%m-%d"),
        "origin": choice(["NYC", "SFO", "LAX", "CHI", "ATL"]),
        "destination": choice(["LON", "PAR", "BER", "AMS", "HKG"]),
        "price_range": randint(300, 1000),
        "booking_status": choice(["Booked", "Not Booked"])
    }
    for i in range(1, 101)
]

# Save to CSV
df_users = pd.DataFrame(users)
df_users.to_csv("user_logs.csv", index=False)
print("user_logs.csv created successfully.")
