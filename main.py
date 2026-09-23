import argparse
import subprocess
import sys
import time

from paths import BRONZE, ROOT

PRODUCER = "etl/producer.py"
CONSUMER = "medallion/01_bronze.py"
BATCH_STAGES = ["medallion/02_silver.py", "medallion/03_gold.py"]

GRACE_PERIOD = 5  


def count_bronze_records():
    if not BRONZE.exists():
        return 0

    total = 0
    for path in BRONZE.glob("settlement_fx_*.json"):
        with open(path) as f:
            total += sum(1 for line in f if line.strip())
    return total


def start_background(script_path):
    print(f"--- Starting (background): {script_path} ---")
    return subprocess.Popen([sys.executable, script_path], cwd=ROOT)


def stop_background(proc, script_path):
    if proc.poll() is not None:
        return

    print(f"--- Stopping: {script_path} ---")
    proc.terminate()

    try:
        proc.wait(timeout=GRACE_PERIOD)
    except subprocess.TimeoutExpired:
        print(f"{script_path} did not exit in {GRACE_PERIOD}s, killing it.")
        proc.kill()
        proc.wait()


def stream_for(duration, target_messages):
    baseline = count_bronze_records()
    print(f"Bronze starting at {baseline} record(s).")

    processes = [
        (start_background(PRODUCER), PRODUCER),
        (start_background(CONSUMER), CONSUMER),
    ]

    deadline = time.monotonic() + duration
    ingested = 0

    try:
        while time.monotonic() < deadline:
            for proc, script_path in processes:
                if proc.poll() is not None:
                    raise RuntimeError(
                        f"{script_path} exited early with code {proc.returncode}."
                    )

            ingested = count_bronze_records() - baseline

            if target_messages is not None and ingested >= target_messages:
                print(f"Reached target of {target_messages} message(s).")
                break

            remaining = int(deadline - time.monotonic())
            print(f"Ingested {ingested} record(s), {remaining}s remaining...")
            time.sleep(1)
        else:
            print(f"Streaming window of {duration}s elapsed.")

    except KeyboardInterrupt:
        print("\nInterrupted, shutting down the streaming stage.")

    finally:
        for proc, script_path in processes:
            stop_background(proc, script_path)

    return count_bronze_records() - baseline


def run_script(script_path):
    print(f"\n--- Starting: {script_path} ---")
    result = subprocess.run([sys.executable, script_path], cwd=ROOT)

    if result.returncode != 0:
        print(f"Error: {script_path} failed with exit code {result.returncode}.")
        sys.exit(result.returncode)
    print(f"--- Finished: {script_path} ---")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="How long to stream into bronze, in seconds (default: 60).",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=None,
        help="Stop streaming early once this many records reach bronze.",
    )
    parser.add_argument(
        "--skip-stream",
        action="store_true",
        help="Skip the streaming stage and only run silver and gold.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.skip_stream:
        print("Skipping the streaming stage.")
    else:
        try:
            ingested = stream_for(args.duration, args.messages)
        except RuntimeError as e:
            print(f"\nError: {e}")
            print("Is RabbitMQ up? Try: docker compose up -d")
            sys.exit(1)

        print(f"\nStreaming stage complete: {ingested} new record(s) in bronze.")

        if ingested == 0 and count_bronze_records() == 0:
            print("Error: bronze is empty, nothing for the batch stages to read.")
            sys.exit(1)

    for stage in BATCH_STAGES:
        run_script(stage)

    print("SUCCESS")


if __name__ == "__main__":
    main()
