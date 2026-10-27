# CMPE 273 Module 5 Assignment - Asynchronous Messaging with RabbitMQ

This project has a producer and a worker that talk to each other through a RabbitMQ queue called `orders`. The producer puts order messages into the queue, and two workers take them out and process them, so they are competing consumers. The workers send a manual ACK when they finish, and they check the event_id first so they can skip a duplicate.

## Setup and Run

I used Windows and Git Bash, and all of these commands are run from the repo root.

First, start RabbitMQ with Docker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

You can look at the queue in the Management UI at http://localhost:15672 (the login is `guest` / `guest`).

Next, make the venv and install the packages. You only have to do this once:

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
```

Each process needs its own terminal, and you have to turn on the venv in every one of them with `source .venv/Scripts/activate`.

The scripts get the RabbitMQ host from the `RABBITMQ_HOST` environment variable, but if you don't set it they just use `localhost`.

Terminal 1 - worker-1:

```bash
python worker.py --name worker-1
```

Terminal 2 - worker-2:

```bash
python worker.py --name worker-2
```

Terminal 3 - producer:

```bash
python producer.py              # sends 6 orders
python producer.py --count 10   # or pick how many to send
```

When you want to stop a worker, just press Ctrl+C.

### Crash test 

For this test you start fresh by deleting the file of processed events. After that, start worker-2 like normal, and start worker-1 with the crash option so it quits on order `o2` before it sends the ACK. Once both workers are running, run the producer.

The order matters here. RabbitMQ hands out messages to the workers in turns, so the first worker gets `o1` and the second one gets `o2`. Starting worker-2 first makes sure worker-1 is the one that gets `o2`.

```bash
rm -f processed_events.txt
python worker.py --name worker-2                         # terminal 1
python worker.py --name worker-1 --crash-before-ack o2   # terminal 2
python producer.py                                       # terminal 3
```

### Duplicate test

This time keep `processed_events.txt` around. The producer sends the 6 orders again, plus one extra message that reuses `evt-1`:

```bash
python producer.py --duplicate evt-1
```

## Architecture

<img width="1920" height="760" alt="architecture" src="https://github.com/user-attachments/assets/d3aaa166-3871-45cd-817d-1304d817a328" />

- The queue is durable and the messages are persistent.
- Each worker uses `prefetch_count=1`, so it only gets one message at a time.
- A worker only sends the ACK after the work is done, so if it dies before the ACK, RabbitMQ gives the message to another worker.
- Both workers read and write the same `processed_events.txt`, which is how they know if an event_id was already done.

## Message Format

```json
{"event_id": "evt-1", "order_id": "o1", "item_id": "item-101", "quantity": 2}
```

## Evidence

All the logs below are real, and I copied them straight from my run.

### Successful message processing

The producer sent 6 orders to the `orders` queue:

```
2026-10-26 19:40:23,208 INFO published event_id=evt-1 order_id=o1 item_id=item-101 quantity=2
2026-10-26 19:40:23,209 INFO published event_id=evt-2 order_id=o2 item_id=item-102 quantity=3
2026-10-26 19:40:23,209 INFO published event_id=evt-3 order_id=o3 item_id=item-103 quantity=1
2026-10-26 19:40:23,209 INFO published event_id=evt-4 order_id=o4 item_id=item-104 quantity=2
2026-10-26 19:40:23,209 INFO published event_id=evt-5 order_id=o5 item_id=item-105 quantity=3
2026-10-26 19:40:23,209 INFO published event_id=evt-6 order_id=o6 item_id=item-106 quantity=1
```

Here is one message going all the way through. worker-1 gets order `o1`, works on it for a second or two, and sends the ACK after it says done:

```
2026-10-26 19:40:23,253 INFO worker=worker-1 received event_id=evt-1 order_id=o1 redelivered=False
2026-10-26 19:40:23,255 INFO worker=worker-1 processing order o1 item_id=item-101 quantity=2
2026-10-26 19:40:25,042 INFO worker=worker-1 done order_id=o1
2026-10-26 19:40:25,051 INFO worker=worker-1 ACK sent event_id=evt-1
```

### Two competing consumers

For this one I started worker-1 and worker-2 on the same queue and ran the producer with 6 messages. The two workers split the work between them, so worker-1 got o1, o4, and o6 while worker-2 got o2, o3, and o5. No message was handled by both workers.

worker-1:

```
2026-10-26 19:40:08,557 INFO worker=worker-1 waiting for messages on queue=orders
2026-10-26 19:40:23,253 INFO worker=worker-1 received event_id=evt-1 order_id=o1 redelivered=False
2026-10-26 19:40:23,255 INFO worker=worker-1 processing order o1 item_id=item-101 quantity=2
2026-10-26 19:40:25,042 INFO worker=worker-1 done order_id=o1
2026-10-26 19:40:25,051 INFO worker=worker-1 ACK sent event_id=evt-1
2026-10-26 19:40:25,053 INFO worker=worker-1 received event_id=evt-4 order_id=o4 redelivered=False
2026-10-26 19:40:25,055 INFO worker=worker-1 processing order o4 item_id=item-104 quantity=2
2026-10-26 19:40:26,943 INFO worker=worker-1 done order_id=o4
2026-10-26 19:40:26,947 INFO worker=worker-1 ACK sent event_id=evt-4
2026-10-26 19:40:26,949 INFO worker=worker-1 received event_id=evt-6 order_id=o6 redelivered=False
2026-10-26 19:40:26,949 INFO worker=worker-1 processing order o6 item_id=item-106 quantity=1
2026-10-26 19:40:28,750 INFO worker=worker-1 done order_id=o6
2026-10-26 19:40:28,750 INFO worker=worker-1 ACK sent event_id=evt-6
```

worker-2:

```
2026-10-26 19:40:08,733 INFO worker=worker-2 waiting for messages on queue=orders
2026-10-26 19:40:23,253 INFO worker=worker-2 received event_id=evt-2 order_id=o2 redelivered=False
2026-10-26 19:40:23,255 INFO worker=worker-2 processing order o2 item_id=item-102 quantity=3
2026-10-26 19:40:24,401 INFO worker=worker-2 done order_id=o2
2026-10-26 19:40:24,410 INFO worker=worker-2 ACK sent event_id=evt-2
2026-10-26 19:40:24,412 INFO worker=worker-2 received event_id=evt-3 order_id=o3 redelivered=False
2026-10-26 19:40:24,412 INFO worker=worker-2 processing order o3 item_id=item-103 quantity=1
2026-10-26 19:40:26,133 INFO worker=worker-2 done order_id=o3
2026-10-26 19:40:26,138 INFO worker=worker-2 ACK sent event_id=evt-3
2026-10-26 19:40:26,141 INFO worker=worker-2 received event_id=evt-5 order_id=o5 redelivered=False
2026-10-26 19:40:26,142 INFO worker=worker-2 processing order o5 item_id=item-105 quantity=3
2026-10-26 19:40:27,839 INFO worker=worker-2 done order_id=o5
2026-10-26 19:40:27,843 INFO worker=worker-2 ACK sent event_id=evt-5
```

### Failure and redelivery

First I deleted `processed_events.txt` and started worker-2 like normal. After that I started worker-1 with `--crash-before-ack o2` and ran the producer.

worker-1 got `o2`, did the work, and wrote `evt-2` to the file, but it quit right before sending the ACK (the exit code was 1).

```
2026-10-26 19:41:41,259 INFO worker=worker-1 waiting for messages on queue=orders
2026-10-26 19:41:48,821 INFO worker=worker-1 received event_id=evt-2 order_id=o2 redelivered=False
2026-10-26 19:41:48,822 INFO worker=worker-1 processing order o2 item_id=item-102 quantity=3
2026-10-26 19:41:50,172 INFO worker=worker-1 done order_id=o2
2026-10-26 19:41:50,182 INFO worker=worker-1 crashing before ACK order_id=o2
```

Since RabbitMQ never got an ACK for `o2`, it sent `o2` again to worker-2, and this time the message had `redelivered=True`. worker-2 saw that `evt-2` was already in the file, so it marked it as a duplicate and didn't do the work again. After that, worker-2 kept running and finished all the other orders, so the system stayed up even though one worker crashed.

```
2026-10-26 19:41:27,265 INFO worker=worker-2 waiting for messages on queue=orders
2026-10-26 19:41:48,820 INFO worker=worker-2 received event_id=evt-1 order_id=o1 redelivered=False
2026-10-26 19:41:48,822 INFO worker=worker-2 processing order o1 item_id=item-101 quantity=2
2026-10-26 19:41:50,622 INFO worker=worker-2 done order_id=o1
2026-10-26 19:41:50,628 INFO worker=worker-2 ACK sent event_id=evt-1
2026-10-26 19:41:50,629 INFO worker=worker-2 received event_id=evt-2 order_id=o2 redelivered=True
2026-10-26 19:41:50,631 INFO worker=worker-2 duplicate detected, skipping event_id=evt-2
2026-10-26 19:41:50,631 INFO worker=worker-2 ACK sent event_id=evt-2
2026-10-26 19:41:50,633 INFO worker=worker-2 received event_id=evt-3 order_id=o3 redelivered=False
2026-10-26 19:41:50,633 INFO worker=worker-2 processing order o3 item_id=item-103 quantity=1
2026-10-26 19:41:51,965 INFO worker=worker-2 done order_id=o3
2026-10-26 19:41:51,975 INFO worker=worker-2 ACK sent event_id=evt-3
2026-10-26 19:41:51,975 INFO worker=worker-2 received event_id=evt-4 order_id=o4 redelivered=False
2026-10-26 19:41:51,975 INFO worker=worker-2 processing order o4 item_id=item-104 quantity=2
2026-10-26 19:41:53,795 INFO worker=worker-2 done order_id=o4
2026-10-26 19:41:53,806 INFO worker=worker-2 ACK sent event_id=evt-4
2026-10-26 19:41:53,806 INFO worker=worker-2 received event_id=evt-5 order_id=o5 redelivered=False
2026-10-26 19:41:53,809 INFO worker=worker-2 processing order o5 item_id=item-105 quantity=3
2026-10-26 19:41:55,398 INFO worker=worker-2 done order_id=o5
2026-10-26 19:41:55,414 INFO worker=worker-2 ACK sent event_id=evt-5
2026-10-26 19:41:55,416 INFO worker=worker-2 received event_id=evt-6 order_id=o6 redelivered=False
2026-10-26 19:41:55,416 INFO worker=worker-2 processing order o6 item_id=item-106 quantity=1
2026-10-26 19:41:56,801 INFO worker=worker-2 done order_id=o6
2026-10-26 19:41:56,809 INFO worker=worker-2 ACK sent event_id=evt-6
```

When it was all over, the queue had 0 messages left and 0 messages waiting for an ACK.

### Duplicate handling

The crash test above already shows one duplicate, because worker-2 got `evt-2` again after the redelivery and skipped it.

For this test I kept `processed_events.txt` and left worker-2 running from before. I started worker-1 again like normal and ran the producer with `--duplicate evt-1`, which sent the 6 orders again plus one more message with `evt-1`:

```
2026-10-26 19:42:25,789 INFO published event_id=evt-1 order_id=o1 item_id=item-101 quantity=2
2026-10-26 19:42:25,790 INFO published event_id=evt-2 order_id=o2 item_id=item-102 quantity=3
2026-10-26 19:42:25,790 INFO published event_id=evt-3 order_id=o3 item_id=item-103 quantity=1
2026-10-26 19:42:25,791 INFO published event_id=evt-4 order_id=o4 item_id=item-104 quantity=2
2026-10-26 19:42:25,791 INFO published event_id=evt-5 order_id=o5 item_id=item-105 quantity=3
2026-10-26 19:42:25,791 INFO published event_id=evt-6 order_id=o6 item_id=item-106 quantity=1
2026-10-26 19:42:25,792 INFO sending duplicate event_id=evt-1
2026-10-26 19:42:25,792 INFO published event_id=evt-1 order_id=o1 item_id=item-101 quantity=2
```

Every event_id was already in the file, so both workers skipped every message and didn't process any order again. They still sent the ACK each time, which is why the messages still left the queue. worker-2 even got `evt-1` two times and skipped it both times.

worker-1:

```
2026-10-26 19:42:23,038 INFO worker=worker-1 waiting for messages on queue=orders
2026-10-26 19:42:25,833 INFO worker=worker-1 received event_id=evt-2 order_id=o2 redelivered=False
2026-10-26 19:42:25,835 INFO worker=worker-1 duplicate detected, skipping event_id=evt-2
2026-10-26 19:42:25,835 INFO worker=worker-1 ACK sent event_id=evt-2
2026-10-26 19:42:25,837 INFO worker=worker-1 received event_id=evt-3 order_id=o3 redelivered=False
2026-10-26 19:42:25,839 INFO worker=worker-1 duplicate detected, skipping event_id=evt-3
2026-10-26 19:42:25,839 INFO worker=worker-1 ACK sent event_id=evt-3
2026-10-26 19:42:25,852 INFO worker=worker-1 received event_id=evt-5 order_id=o5 redelivered=False
2026-10-26 19:42:25,852 INFO worker=worker-1 duplicate detected, skipping event_id=evt-5
2026-10-26 19:42:25,853 INFO worker=worker-1 ACK sent event_id=evt-5
```

worker-2:

```
2026-10-26 19:42:25,833 INFO worker=worker-2 received event_id=evt-1 order_id=o1 redelivered=False
2026-10-26 19:42:25,835 INFO worker=worker-2 duplicate detected, skipping event_id=evt-1
2026-10-26 19:42:25,835 INFO worker=worker-2 ACK sent event_id=evt-1
2026-10-26 19:42:25,838 INFO worker=worker-2 received event_id=evt-4 order_id=o4 redelivered=False
2026-10-26 19:42:25,839 INFO worker=worker-2 duplicate detected, skipping event_id=evt-4
2026-10-26 19:42:25,839 INFO worker=worker-2 ACK sent event_id=evt-4
2026-10-26 19:42:25,847 INFO worker=worker-2 received event_id=evt-6 order_id=o6 redelivered=False
2026-10-26 19:42:25,848 INFO worker=worker-2 duplicate detected, skipping event_id=evt-6
2026-10-26 19:42:25,849 INFO worker=worker-2 ACK sent event_id=evt-6
2026-10-26 19:42:25,852 INFO worker=worker-2 received event_id=evt-1 order_id=o1 redelivered=False
2026-10-26 19:42:25,852 INFO worker=worker-2 duplicate detected, skipping event_id=evt-1
2026-10-26 19:42:25,853 INFO worker=worker-2 ACK sent event_id=evt-1
```

Here is what `processed_events.txt` looked like at the end. Each event_id shows up only one time, and `evt-2` is at the top because worker-1 wrote it right before it crashed.

```
evt-2
evt-1
evt-3
evt-4
evt-5
evt-6
```

## Note About the File

There is a small race in my setup, because if two workers check the file at the exact same time, they might both process the same event. A real system would use a database with a unique key and save the result and the event_id together in one step.

## Reflection

A message can be delivered more than once because RabbitMQ only removes a message after it gets an ACK. If a worker finishes the work but crashes before sending the ACK, RabbitMQ thinks the message was never handled and sends it to another worker. The network can also fail, so the producer might send the same message again. Idempotent handling helps because the worker saves each event_id and checks it first, so the same event only changes things one time. In my test, worker-1 crashed before the ACK, and worker-2 got the message again but skipped it as a duplicate.
