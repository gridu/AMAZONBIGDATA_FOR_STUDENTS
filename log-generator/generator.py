"""
This script generates data for a capstone project.

There are three modes of operation:

`batch` - generates a batch of logs with timestamps from the previous hour in CSV format and uploads it to S3
`stream items` - streams item view logs continuously into Kinesis
`stream reviews` - streams review logs continuously into Kinesis

Note, that this script stores generated users and items in CSV files in its working directory and automatically picks
it up from there, so items/users persist across multiple runs. To re-generate them, remove/rename these CSV files.

Most of the behaviour can be controlled by tweaking global variables below. You'll need to update some of them
(Kinesis stream/S3 bucket) to your own.
"""

import csv
import json
import os
import random
import time
import shutil
import subprocess
import sys

from datetime import datetime, timedelta

S3_BUCKET = "<your S3 bucket>"
S3_PREFIX = "<your prefix>"

KINESIS_ITEM_STREAM = "<your kinesis stream for item purchase events>"
KINESIS_REVIEW_STREAM = "<your kinesis stream for review events>"

N_USERS = 50
N_ITEMS = 100
BOT_PROBABILITY = 0.1

ITEMS_FILE = "items.csv"
USERS_FILE = "users.csv"

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_review_texts(path):
    if not os.path.exists(path):
        subprocess.check_call([
            "curl",
            "https://archive.ics.uci.edu/ml/machine-learning-databases/00228/smsspamcollection.zip",
            "-o",
            "/tmp/smsspamcollection.zip",
        ])

        subprocess.check_call([
            "unzip", "-o", "/tmp/smsspamcollection.zip", "-d", "/tmp/dataset"
        ])

        shutil.move("/tmp/dataset/SMSSpamCollection", path)
        os.remove("/tmp/smsspamcollection.zip")
        shutil.rmtree("/tmp/dataset", ignore_errors=True)

    with open(path) as f:
        lines = [line.split("\t", 1)[-1].lower() for line in f.read().splitlines()]

    return lines


REVIEWS = get_review_texts("review_texts.tsv")


def generate_device():
    user_ip = f"{random.randint(11, 191)}.{random.randint(1, 223)}." \
              f"{random.randint(1, 254)}.{random.randint(1, 254)}"
    device_type = random.choice(["mobile:ios", "mobile:android", "other"])
    device_id = random.randint(100000, 1000000) if device_type != "other" else "NULL"
    is_bot = str(1 if random.random() < BOT_PROBABILITY else 0)
    return user_ip, device_type, str(device_id), is_bot


def generate_item(item_id):
    title = "item #{}".format(item_id)
    description = "description of item {}".format(item_id)
    category = random.randint(1, 13)
    return str(item_id), title, description, str(category)


def generate_or_load(filename, generator, header=None, force=False):
    if os.path.exists(filename) and not force:
        with open(filename) as f:
            items = [line.split(",") for line in f.read().splitlines()][1 if header else 0:]
    else:
        items = generator()
        with open(filename, "w") as f:
            if header is not None:
                f.write(header + '\n')
            f.write("\n".join((",".join(i) for i in items)))
    return items


def generate_or_load_users(n, force=False):
    return generate_or_load(USERS_FILE, lambda: [
        (k, *v)
        for k, v in {
            i: (t, di, bot)
            for i, t, di, bot in {generate_device() for _ in range(n)}
        }.items()
    ], force=force)


def generate_or_load_items(n):
    return generate_or_load(ITEMS_FILE, lambda: [
        generate_item(i)
        for i in range(1000, 1000 + n)
    ], header="item_id,title,description,category", force=False)


def generate_logs(users, start_time, line_generator, min_lines=100, max_lines=1000, max_bot_lines=None):
    users = [(*u[:-1], int(u[-1]) == 1) for u in users]
    logs = []
    n_bots = sum((u[-1] for u in users))
    n_bot_lines = 0
    for user_ip, device_type, device_id, is_bot in users:
        ts = start_time
        ts_to = start_time + timedelta(hours=1)
        n_lines = random.randint(min_lines, max_lines)
        ts_step = timedelta(seconds=3600/n_lines)

        if not is_bot:
            for _ in range(n_lines):
                line = line_generator(ts, device_type, device_id, user_ip)

                logs.append(line)
                ts += ts_step

                if ts > ts_to:
                    break
        else:
            this_bot_lines = 0
            while True:
                line = line_generator(ts, device_type, device_id, user_ip)

                logs.append(line)
                this_bot_lines += 1
                if max_bot_lines is not None and this_bot_lines > max_bot_lines:
                    break

                if random.random() < 0.15:
                    ts += ts_step
                else:
                    ts += timedelta(milliseconds=16)

                if ts > ts_to:
                    break
            n_bot_lines += this_bot_lines

    logs.sort(key=lambda l: l["ts"])

    logs = [json.dumps(line, sort_keys=True, separators=(',', ':')) for line in logs]

    print("timestamp\t# lines\t# bots\tbot IPs:")
    print(f"{start_time.strftime(TS_FORMAT)} {len(logs)} {n_bots} "
          f"{','.join((l[0] for l in users if l[-1]))}", file=sys.stderr)

    return logs


def line_generator_item_access(ts, device_type, device_id, user_ip, items):
    item_id = random.choice(items)[0]
    return dict(
        item_id=item_id,
        ts=ts.strftime(TS_FORMAT),
        device_type=device_type,
        device_id=device_id,
        user_ip=user_ip,
    )


def line_generator_review(ts, device_type, device_id, user_ip, items):
    item_id = random.choice(items)[0]
    review = random.choice(REVIEWS).split()
    title = " ".join(review[:3])
    text = " ".join(review[3:])
    return dict(
        item_id=item_id,
        ts=ts.strftime(TS_FORMAT),
        device_type=device_type,
        device_id=device_id,
        user_ip=user_ip,
        # review_title, review_text, review_stars
        review_title=title,
        review_text=text,
        review_stars=random.randint(0, 5)
    )


def sleep_until(t):
    now = datetime.now()
    if t > now:
        time.sleep((t - now).total_seconds())


def stream_to_kinesis(stream, log_generator):
    import boto3
    from kiner.producer import KinesisProducer

    producer = KinesisProducer(
        stream_name=stream,
        batch_size=100,
        batch_time=1,
        max_retries=10,
        kinesis_client=boto3.client("kinesis", region_name="us-east-1")
    )

    try:
        while True:
            logs = log_generator()

            for record in logs:
                next_record_ts = datetime.strptime(json.loads(record)["ts"], TS_FORMAT)
                sleep_until(next_record_ts)
                print(record)
                producer.put_record(record + "\n")
    finally:
        producer.close()


def generate_batch(users, items):
    import boto3

    prev_hour = datetime.now().replace(microsecond=0, second=0, minute=0) - timedelta(hours=1)
    logs = generate_logs(users, prev_hour, lambda *a: line_generator_item_access(*a, items=items))

    with open("batch.csv", "w") as batch:
        logs = map(json.loads, logs)
        first, rest = next(logs), logs
        writer = csv.DictWriter(batch, fieldnames=first.keys())
        writer.writeheader()
        writer.writerow(first)
        writer.writerows(rest)
    boto3.client("s3").upload_file(
        "batch.csv", S3_BUCKET, S3_PREFIX + prev_hour.strftime("/%Y/%m/%d/%H.csv")
    )


def main(mode, stream=None):
    items = generate_or_load_items(N_ITEMS)
    users = generate_or_load_users(N_USERS)

    if mode == "batch":
        generate_batch(users, items)
    elif mode == "stream":
        if stream == "items":
            stream_name = KINESIS_ITEM_STREAM

            def log_generator():
                new_now = datetime.now()
                return generate_logs(users, new_now, lambda *a: line_generator_item_access(*a, items=items))
        elif stream == "reviews":
            stream_name = KINESIS_REVIEW_STREAM

            def log_generator():
                new_now = datetime.now()
                return generate_logs(users, new_now, lambda *a: line_generator_review(*a, items=items))
        else:
            raise ValueError(f"Stream should be one of (items, reviews), got {stream}")

        try:
            stream_to_kinesis(stream_name, log_generator)
        except KeyboardInterrupt:
            print("stop streaming", file=sys.stderr)
    else:
        raise ValueError(f"First argument (mode) should be one of (stream, batch), got {mode}")


if __name__ == "__main__":
    main(*sys.argv[1:])
