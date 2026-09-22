import json
import random
import time
from datetime import datetime
import pika
import requests
from utils import generate_merchant_pool
from dotenv import load_dotenv
import os

def publish_fx_settlement_data():
    load_dotenv()
    FX_API_KEY = os.getenv("EXCHANGE_RATE_API_KEY")
    API_URL = f"https://v6.exchangerate-api.com/v6/{FX_API_KEY}/latest/MYR"

    MERCHANT_POOL = generate_merchant_pool(num_merchants=25, output_path="../data/merchant_dim.json")

    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="settlement_fx_queue")

    while True:
        try:
            response = requests.get(API_URL)

            if response.status_code == 200:
                data = response.json()

                if data.get("result") == "success":
                    rates = data.get("rates",{})
                    target_currencies = ["EUR", "GBP", "USD", "JPY", "CHF"]
                    currency_choice = random.choice(target_currencies)

                    if currency_choice in rates:
                        live_rate = rates[currency_choice]
                        merchant = random.choice(MERCHANT_POOL)
                        gross_amount = round(random.uniform(200.0, 15000.0),2)

                        transaction = {
                            "merchant_id": merchant["merchant_id"],
                            "source_currency": "MYR",
                            "target_currency": currency_choice,
                            "gross_amount": gross_amount,
                            "applied_fx_rate": live_rate,
                            "converted_target_amount": round(gross_amount * live_rate, 2),
                            "provider_timestamp": data.get("time_last_update_utc"),
                            "ingestion_timestamp": datetime.utcnow().isoformat(),
                            "status": random.choices(["SETTLED","PENDING","FAILED"])[0]
                        }

                        channel.basic_publish(exchange='', routing_key="settlement_fx_queue", body=json.dumps(transaction))

                    else:
                        print(f"API error response: {data.get('error_type')}")

                else:
                    print(f"HTTP Error: {response.status_code}")

                time.sleep(10)

        except Exception as e:
            print(e)
            time.sleep(5)

if __name__ == "__main__":
    publish_fx_settlement_data()

    





