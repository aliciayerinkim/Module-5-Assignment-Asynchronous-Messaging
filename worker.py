import argparse
import json
import logging
import os
import random
import time

import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")
logging.getLogger("pika").setLevel(logging.WARNING)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
QUEUE = "orders"
# shared by all workers, sits next to this file
PROCESSED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "processed_events.txt")


def already_processed(event_id):
    if not os.path.exists(PROCESSED_FILE):
        return False
    with open(PROCESSED_FILE) as f:
        return event_id in {line.strip() for line in f}


def mark_processed(event_id):
    with open(PROCESSED_FILE, "a") as f:
        f.write(event_id + "\n")
        f.flush()
        os.fsync(f.fileno())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="worker", help="worker name for the logs")
    parser.add_argument("--crash-before-ack", metavar="ORDER_ID",
                        help="exit before sending the ACK for this order")
    args = parser.parse_args()
    name = args.name

    def on_message(ch, method, properties, body):
        order = json.loads(body)
        event_id = order["event_id"]
        order_id = order["order_id"]
        log.info("worker=%s received event_id=%s order_id=%s redelivered=%s",
                 name, event_id, order_id, method.redelivered)

        if already_processed(event_id):
            log.info("worker=%s duplicate detected, skipping event_id=%s", name, event_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            log.info("worker=%s ACK sent event_id=%s", name, event_id)
            return

        log.info("worker=%s processing order %s item_id=%s quantity=%s",
                 name, order_id, order["item_id"], order["quantity"])
        time.sleep(random.uniform(1, 2))
        log.info("worker=%s done order_id=%s", name, order_id)
        mark_processed(event_id)

        if args.crash_before_ack == order_id:
            log.info("worker=%s crashing before ACK order_id=%s", name, order_id)
            os._exit(1)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("worker=%s ACK sent event_id=%s", name, event_id)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)

    log.info("worker=%s waiting for messages on queue=%s", name, QUEUE)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("worker=%s stopping (Ctrl+C)", name)
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()
