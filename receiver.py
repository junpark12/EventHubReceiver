import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

from azure.eventhub import EventData
from azure.eventhub.exceptions import EventHubError
from azure.eventhub import EventHubConsumerClient
from dotenv import load_dotenv


def _get_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _safe_filename(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in value)


def _event_body_to_text(event: EventData) -> str:
    try:
        return event.body_as_str(encoding="UTF-8")
    except Exception:
        raw = b"".join(event.body)
        return raw.decode("utf-8", errors="replace")


def _to_json_safe(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        normalized = {}
        for k, v in value.items():
            key = _to_json_safe(k)
            if not isinstance(key, (str, int, float, bool)) and key is not None:
                key = str(key)
            normalized[key] = _to_json_safe(v)
        return normalized
    if isinstance(value, (list, tuple, set)):
        return [_to_json_safe(v) for v in value]
    return value


def main() -> None:
    load_dotenv()

    connection_string = _get_required("EVENTHUB_CONNECTION_STRING")
    eventhub_name = _get_required("EVENTHUB_NAME")
    consumer_group = os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default")
    output_dir = Path(os.getenv("OUTPUT_DIR", "./output"))

    max_events = int(os.getenv("MAX_EVENTS", "100"))
    run_duration_seconds = int(os.getenv("RUN_DURATION_SECONDS", "60"))
    max_wait_time = int(os.getenv("MAX_WAIT_TIME_SECONDS", "10"))
    starting_position = os.getenv("STARTING_POSITION", "-1")  # -1: earliest, @latest: latest

    output_dir.mkdir(parents=True, exist_ok=True)

    stop_event = threading.Event()
    received_count = 0

    def on_event(partition_context, event: EventData):
        nonlocal received_count

        if event is None:
            return

        body_text = _event_body_to_text(event)
        try:
            body_json = json.loads(body_text)
        except json.JSONDecodeError:
            body_json = {"raw_body": body_text}

        payload = {
            "receivedAtUtc": datetime.now(timezone.utc).isoformat(),
            "eventHubName": eventhub_name,
            "consumerGroup": consumer_group,
            "partitionId": partition_context.partition_id,
            "sequenceNumber": event.sequence_number,
            "offset": event.offset,
            "enqueuedTimeUtc": event.enqueued_time.isoformat() if event.enqueued_time else None,
            "systemProperties": _to_json_safe(dict(event.system_properties)) if event.system_properties else {},
            "properties": _to_json_safe(dict(event.properties)) if event.properties else {},
            "body": _to_json_safe(body_json),
        }

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        filename = (
            f"{_safe_filename(eventhub_name)}"
            f"_p{partition_context.partition_id}"
            f"_seq{event.sequence_number}_{ts}.json"
        )
        output_path = output_dir / filename
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        received_count += 1
        print(f"[RECEIVED] partition={partition_context.partition_id} seq={event.sequence_number} -> {output_path}")

        if max_events > 0 and received_count >= max_events:
            print(f"[INFO] Reached MAX_EVENTS={max_events}. Stopping receiver.")
            stop_event.set()

    def on_error(partition_context, error: EventHubError):
        partition = partition_context.partition_id if partition_context else "N/A"
        print(f"[ERROR] partition={partition} error={error}")

    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_string,
        consumer_group=consumer_group,
        eventhub_name=eventhub_name,
    )

    timer = threading.Timer(run_duration_seconds, stop_event.set)
    timer.start()

    print(
        "[INFO] Starting receiver "
        f"(eventhub={eventhub_name}, consumer_group={consumer_group}, "
        f"starting_position={starting_position}, run_duration={run_duration_seconds}s, "
        f"max_events={max_events}, output_dir={output_dir})"
    )

    try:
        with client:
            client.receive(
                on_event=on_event,
                on_error=on_error,
                starting_position=starting_position,
                max_wait_time=max_wait_time,
                stop_event=stop_event,
            )
    finally:
        timer.cancel()

    print(f"[DONE] Received events: {received_count}")


if __name__ == "__main__":
    main()
