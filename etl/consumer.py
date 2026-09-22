import pika
import json
import os
from datetime import datetime
from dotenv import load_dotenv

def consume_data():
    load_dotenv()

    RABBITMQ_USER=os.getenv("RABBIT_MQ_USER")
    RABBITMQ_PASSWORD=os.getenv("RABBIT_MQ_PASSWORD")

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue="settlement_fx_queue", durable=True)

    BRONZE_PATH = "../data/bronze"
    os.makedirs(BRONZE_PATH, exist_ok=True)

    def callback(ch, method, properties, body):
        try:
            raw_json = body.decode("utf-8")
            json.loads(raw_json)
            date = datetime.utcnow().strftime("%Y-%m-%d")

            file_path = os.path.join(BRONZE_PATH,f"settlement_fx_{date}.json")

            with open(file_path,"a") as f:
                f.write(raw_json + "\n")

            print("Stored: ", raw_json)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f"Error: {e}")

            ch.basic_ack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue="settlement_fx_queue", on_message_callback=callback, auto_ack=False)
    channel.start_consuming()

if __name__ == "__main__":
    consume_data()

