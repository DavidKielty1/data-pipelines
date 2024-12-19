import pandas as pd
from random import randint, choice
from datetime import datetime, timedelta

# Generate mock flight data
flights = [
    {
        "flight_id": f"FL{randint(1000, 9999)}",
        "departure_date": (datetime.now() + timedelta(days=randint(1, 30))).strftime("%Y-%m-%d"),
        "origin": choice(["NYC", "SFO", "LAX", "CHI", "ATL"]),
        "destination": choice(["LON", "PAR", "BER", "AMS", "HKG"]),
        "price": randint(200, 1500),
        "availability": choice(["High", "Medium", "Low"])
    }
    for _ in range(200)
]

# Save to CSV
df_flights = pd.DataFrame(flights)
df_flights.to_csv("flights.csv", index=False)
print("flights.csv created successfully.")
