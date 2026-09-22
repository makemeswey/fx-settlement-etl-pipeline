import json
import random
from faker import Faker

def generate_merchant_pool(num_merchants=25, output_path="../data/merchant_dim.json"):
    fake = Faker()
    merchant_pool = []

    for _ in range(num_merchants):
        merchant_pool.append({
            "merchant_id": f"MERCH_{fake.random_int(min=1000, max=9999)}",
            "merchant_name": fake.company(),
            "tier": random.choices(["Enterprise","MNC","Startup"], weights=[15,35,50])[0],
            "custom_fee_rate": round(random.uniform(0.008,0.022),4)
        })

    with open(output_path, "a") as f:
        json.dump(merchant_pool, f)

    return merchant_pool

if __name__ == "__main__":
    generate_merchant_pool()
