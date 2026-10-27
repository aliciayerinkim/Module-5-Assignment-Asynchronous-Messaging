import argparse
import json
import logging
import os

import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")
logging.getLogger("pika").setLevel(logging.WARNING)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
QUEUE = "orders"


def make_order(n):
    return {
        "event_id": f"evt-{n}",
        "order_id": f"o{n}",
        "item_id": f"item-{100 + n}",
        "quantity": n % 3 + 1,
    }


def publish(channel, order):
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=json.dumps(order),
        # delivery_mode=2 makes the message persistent
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )
    log.info("published event_id=%s order_id=%s item_id=%s quantity=%s",
             order["event_id"], order["order_id"], order["item_id"], order["quantity"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=6, help="how many orders to publish")
    parser.add_argument("--duplicate", metavar="EVENT_ID",
                        help="also publish one more message that reuses this event_id")
    args = parser.parse_args()

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)

    for n in range(1, args.count + 1):
        publish(channel, make_order(n))

    if args.duplicate:
        # resend the same order as the original event, like a producer retry
        num = args.duplicate.removeprefix("evt-")
        if num.isdigit():
            order = make_order(int(num))
        else:
            order = {"event_id": args.duplicate, "order_id": "o-dup", "item_id": "item-100", "quantity": 1}
        log.info("sending duplicate event_id=%s", order["event_id"])
        publish(channel, order)

    connection.close()


if __name__ == "__main__":
    main()
