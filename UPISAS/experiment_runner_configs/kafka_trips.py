# Reads single trips from the Kafka topic crowd-nav-trips, for the experiment results.
# The strategy does not use this. The HTTP API only gives the running mean,
# and for the median we need the single trips, which are only in Kafka.
# The topic holds the trips of all runs in a row, so we note the end offset
# before a run and read from there after it.
import json
import subprocess
import time

KAFKA_BIN = "/opt/kafka_2.11-0.10.1.0/bin"
TOPIC = "crowd-nav-trips"


def _docker_exec(arguments, timeout):
    """Runs a command inside the kafka container, returns its output ("" if it fails)."""
    try:
        result = subprocess.run(["docker", "exec", "kafka"] + arguments,
                                capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return ""
    return result.stdout


def _end_offset():
    """Number of trip messages in the topic so far, None if Kafka does not answer."""
    text = _docker_exec([KAFKA_BIN + "/kafka-run-class.sh", "kafka.tools.GetOffsetShell",
                         "--broker-list", "localhost:9092", "--topic", TOPIC, "--time", "-1"], timeout=60)
    for line in text.splitlines():
        if line.startswith(TOPIC + ":0:"):
            return int(line.split(":")[2])
    return None


def topic_line_count(tries=6):
    """How many trips the topic holds. 0 if Kafka gives nothing."""
    # crowdnav has to be stopped, otherwise the end keeps moving
    # so we ask until we get the same answer twice
    previous = None
    for attempt in range(tries):
        count = _end_offset()
        if count is not None and count == previous:
            return count
        previous = count
        time.sleep(1.5 if count is not None else 5)
    print("kafka_trips: the end offset could not be read or kept moving")
    return previous or 0


def read_new_trips(count_before, expected_at_least, tries=4):
    """The trips after the first count_before messages, as dicts. None if Kafka does not deliver."""
    for attempt in range(tries):
        count_now = topic_line_count()
        new = count_now - count_before
        if new >= expected_at_least and new > 0:
            text = _docker_exec([KAFKA_BIN + "/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092",
                                 "--topic", TOPIC, "--partition", "0", "--offset", str(count_before),
                                 "--max-messages", str(new), "--timeout-ms", "20000"], timeout=240)
            lines = [line for line in text.splitlines() if line.startswith("{")]
            if len(lines) == new:
                return [json.loads(line) for line in lines]
            print("kafka_trips: read %d lines, expected %d (try %d of %d)" % (len(lines), new, attempt + 1, tries))
        else:
            print("kafka_trips: the topic has %d new messages, expected at least %d (try %d of %d)"
                  % (new, expected_at_least, attempt + 1, tries))
        time.sleep(10)
    return None
