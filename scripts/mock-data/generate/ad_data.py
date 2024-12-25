import pandas as pd
from random import randint, choice

# Generate mock ad inventory
ads = [
    {
        "ad_id": f"AD{randint(100, 999)}",
        "target_segment": choice(["Budget Travelers", "Frequent Travelers", "Business Class"]),
        "ad_content": choice([
            "Special Discounts on Flights!",
            "Exclusive Business Class Offers!",
            "Luxury Hotel Deals",
            "Budget-Friendly Vacation Packages",
        ]),
        "click_through_rate": round(randint(10, 80) / 100, 2)  # CTR as a percentage
    }
    for _ in range(50)
]

# Save to CSV
df_ads = pd.DataFrame(ads)
df_ads.to_csv("ads.csv", index=False)
print("ads.csv created successfully.")
