import json
import random
import sys
from pathlib import Path

from faker import Faker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from paths import MERCHANT_DIM

def generate_merchant_pool(num_merchants=25, output_path=MERCHANT_DIM):
    fake = Faker()
    merchant_pool = []

    for _ in range(num_merchants):
        merchant_pool.append({
            "merchant_id": f"MERCH_{fake.random_int(min=1000, max=9999)}",
            "merchant_name": fake.company(),
            "tier": random.choices(["Enterprise","MNC","Startup"], weights=[15,35,50])[0],
            "custom_fee_rate": round(random.uniform(0.008,0.022),4)
        })

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(merchant_pool, f)

    return merchant_pool

if __name__ == "__main__":
    generate_merchant_pool()
