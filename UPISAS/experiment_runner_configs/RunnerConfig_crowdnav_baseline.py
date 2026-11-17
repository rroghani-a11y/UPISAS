# Experiment Runner config for the baseline: RTX step strategy vs no adaptation, on CrowdNav.
# Run it from the repo root, with the CrowdNav stack up:
#   python experiment-runner/experiment-runner/ UPISAS/experiment_runner_configs/RunnerConfig_crowdnav_baseline.py
#
# One run = restart the crowdnav container (so it starts from knobs.json), one round every 2 s
# until LAST_STEP, then read the run's trips from Kafka and fill one row of the run table.
import json
import os
import statistics
import subprocess
import sys
import time
from os.path import dirname, realpath
from pathlib import Path
from typing import Dict, Optional

import requests

from EventManager.Models.RunnerEvents import RunnerEvents
from EventManager.EventSubscriptionController import EventSubscriptionController
from ConfigValidator.Config.Models.RunTableModel import RunTableModel
from ConfigValidator.Config.Models.FactorModel import FactorModel
from ConfigValidator.Config.Models.RunnerContext import RunnerContext
from ConfigValidator.Config.Models.OperationType import OperationType
from ExtendedTyping.Typing import SupportsStr
from ProgressManager.Output.OutputProcedure import OutputProcedure as output

# repo root is two folders up, kafka_trips.py sits next to this file
HERE = Path(dirname(realpath(__file__)))
UPISAS_REPO = Path(os.environ.get("UPISAS_REPO", HERE.parent.parent))
sys.path.insert(0, str(UPISAS_REPO))
sys.path.insert(0, str(HERE))

from UPISAS.exemplars.crowdnav import CrowdNav
from UPISAS.strategies.crowdnav_rtx_step import RtxStepStrategy, SCALED
from kafka_trips import topic_line_count, read_new_trips


# ================================ SETTINGS ================================
# one tenth of the paper's sample sizes (skip 100 trips, sample 500), so the sweep fits in 10,000 steps
REPETITIONS = 5
PARAMETERS = SCALED
LAST_STEP = 10000             # run stops here, trips count up to here
ROUND_SECONDS = 2             # pause between two rounds of the loop
FIRST_START_STEP = 20         # commands sent before tick 10 get lost
LATEST_START_STEP = 300       # first reading of a new run can be late, we saw up to step 257
STUCK_SECONDS = 90            # step not moving for this long = run failed

API = "http://localhost:8080"
CROWDNAV_CONTAINER = os.environ.get("CROWDNAV_CONTAINER", "crowdnav")

DATA_COLUMNS = (
    ["status", "median_overhead", "mean_overhead", "p90_overhead", "complaint_share",
     "monitor_final_running_mean", "routing_duration_mean_after_1000", "trips"]
    + ["sample_mean_%s" % value for value in RtxStepStrategy.EXPLORATION_VALUES]
    + ["seen_in_configs_step_%s" % value for value in RtxStepStrategy.EXPLORATION_VALUES]
    + ["sweep_finished", "chosen_exploration", "chosen_applied_step", "wall_seconds"]
)


# ================================ HELPERS ================================
def summarise_trips(trips):
    """Median, mean, 90th percentile and complaint share of the trips up to LAST_STEP."""
    scored = [trip for trip in trips if trip["tick"] <= LAST_STEP]
    overheads = sorted(trip["overhead"] for trip in scored)
    return {
        "trips": len(overheads),
        "median_overhead": round(statistics.median(overheads), 4),
        "mean_overhead": round(statistics.mean(overheads), 4),
        "p90_overhead": round(overheads[int(0.9 * (len(overheads) - 1))], 4),
        "complaint_share": round(sum(trip["complaint"] for trip in scored) / len(scored), 4),
    }


def run_docker(*arguments):
    """Runs one docker command, e.g. run_docker("restart", "kafka")."""
    print("$ docker " + " ".join(arguments))
    return subprocess.run(["docker"] + list(arguments), capture_output=True, text=True, timeout=240)


def read_step():
    """Step of the newest /monitor/ message, or None."""
    try:
        data = requests.get(API + "/monitor/", timeout=5).json()
        return data["car_stats"]["step"]
    except Exception:
        return None


def wait_for_new_run(stale_step, max_seconds=90):
    """Step of the first message of the new run, or None."""
    # right after the restart /monitor/ still shows the old run's last message
    # so only take a low step that is also below the old run's last step
    start = time.time()
    while time.time() - start < max_seconds:
        step = read_step()
        if step is not None and FIRST_START_STEP <= step <= LATEST_START_STEP:
            if stale_step is None or step < stale_step:
                return step
        time.sleep(0.2)
    return None


class RunnerConfig:
    ROOT_DIR = Path(dirname(realpath(__file__)))

    # ================================ USER SPECIFIC CONFIG ================================
    """The name of the experiment."""
    name:                       str             = "crowdnav_rtx_step_baseline"

    """Results go to <this folder>/experiments/<name>/ (run_table.csv and one folder per run)."""
    results_output_path:        Path            = ROOT_DIR / 'experiments'

    operation_type:             OperationType   = OperationType.AUTO

    time_between_runs_in_ms:    int             = 1000

    exemplar = None
    strategy = None

    def __init__(self):
        """Executes immediately after program start, on config load. No Docker here."""
        EventSubscriptionController.subscribe_to_multiple_events([
            (RunnerEvents.BEFORE_EXPERIMENT, self.before_experiment),
            (RunnerEvents.BEFORE_RUN       , self.before_run       ),
            (RunnerEvents.START_RUN        , self.start_run        ),
            (RunnerEvents.START_MEASUREMENT, self.start_measurement),
            (RunnerEvents.INTERACT         , self.interact         ),
            (RunnerEvents.STOP_MEASUREMENT , self.stop_measurement ),
            (RunnerEvents.STOP_RUN         , self.stop_run         ),
            (RunnerEvents.POPULATE_RUN_DATA, self.populate_run_data),
            (RunnerEvents.AFTER_EXPERIMENT , self.after_experiment )
        ])
        self.run_table_model = None
        output.console_log("CrowdNav baseline config loaded")

    def create_run_table_model(self) -> RunTableModel:
        """Two treatments, five repetitions each, in shuffled order."""
        treatment = FactorModel("treatment", ["no_adaptation", "rtx_step"])
        self.run_table_model = RunTableModel(
            factors=[treatment],
            repetitions=REPETITIONS,
            data_columns=DATA_COLUMNS,
            shuffle=True,
        )
        return self.run_table_model

    def before_experiment(self) -> None:
        output.console_log("Config.before_experiment() called!")

    def before_run(self) -> None:
        """Makes a fresh exemplar and strategy for every run."""
        self.exemplar = CrowdNav()
        self.strategy = RtxStepStrategy(self.exemplar, PARAMETERS)
        self.readings = []
        self.error = None
        output.console_log("Config.before_run() called!")

    def start_run(self, context: RunnerContext) -> None:
        """Counts the trips already in Kafka, then starts a new CrowdNav run and waits for it."""
        run_docker("kill", CROWDNAV_CONTAINER)      # Kafka can only be read while crowdnav is stopped
        self.trips_before = topic_line_count()
        stale_step = read_step()            # the old run's last step
        self.start_time = time.time()
        run_docker("restart", CROWDNAV_CONTAINER)
        self.start_step = wait_for_new_run(stale_step)
        if self.start_step is None:
            self.error = "no new run seen on /monitor/ within 90 s"
        output.console_log("Config.start_run() called! New run seen at step %s" % self.start_step)

    def start_measurement(self, context: RunnerContext) -> None:
        output.console_log("Config.start_measurement() called!")

    def interact(self, context: RunnerContext) -> None:
        """One round every ROUND_SECONDS until step LAST_STEP."""
        if self.error:
            return
        treatment = context.run_variation["treatment"]
        last_step, last_move = None, time.time()
        failed_reads = 0
        try:
            while True:
                try:
                    self.strategy.monitor(with_validation=False)
                except Exception as error:   # /monitor/ was null or did not answer
                    failed_reads += 1
                    if failed_reads > 10:
                        self.error = "monitor failed more than 10 times in a row: %r" % error
                        return
                    time.sleep(ROUND_SECONDS)
                    continue
                failed_reads = 0
                car_stats = self.strategy.knowledge.monitored_data["car_stats"][-1]
                configs = self.strategy.knowledge.monitored_data["configs"][-1]
                self.readings.append({
                    "seconds": round(time.time() - self.start_time, 1),
                    "step": car_stats["step"],
                    "total_trips": car_stats["total_trips"],
                    "total_trip_overhead_average": car_stats["total_trip_overhead_average"],
                    "routing_duration": car_stats["routing_duration"],
                    "driving_car_counter": car_stats["driving_car_counter"],
                    "exploration_percentage": configs["exploration_percentage"],
                    "configs": dict(configs),
                })
                if car_stats["step"] >= LAST_STEP:
                    break

                if treatment == "rtx_step":
                    if self.strategy.analyze():
                        if self.strategy.plan():
                            # validation off: the schema allows edge_average_influence only up to 1,
                            # but CrowdNav really runs with 140
                            self.strategy.execute(with_validation=False)

                if car_stats["step"] != last_step:
                    last_step, last_move = car_stats["step"], time.time()
                elif time.time() - last_move > STUCK_SECONDS:
                    self.error = "simulation stuck at step %s" % last_step
                    return
                time.sleep(ROUND_SECONDS)
        except Exception as error:
            self.error = "exception in the loop: %r" % error
        output.console_log("Config.interact() called!")

    def stop_measurement(self, context: RunnerContext) -> None:
        output.console_log("Config.stop_measurement called!")

    def stop_run(self, context: RunnerContext) -> None:
        """Stops the simulation, it does not stop by itself."""
        self.wall_seconds = round(time.time() - self.start_time, 1)
        run_docker("kill", CROWDNAV_CONTAINER)
        output.console_log("Config.stop_run() called!")

    def populate_run_data(self, context: RunnerContext) -> Optional[Dict[str, SupportsStr]]:
        """Reads this run's trips from Kafka and fills one row of the run table."""
        treatment = context.run_variation["treatment"]
        knowledge = self.strategy.knowledge.analysis_data
        row = {column: "" for column in DATA_COLUMNS}
        row["wall_seconds"] = self.wall_seconds

        trips = None
        if self.readings:
            expected = max(0, self.readings[-1]["total_trips"] - 3)
            trips = read_new_trips(self.trips_before, expected)

        problems = [self.error] if self.error else []
        if trips is None:
            problems.append("the trips could not be read from Kafka")

        if trips:
            try:
                row.update(summarise_trips(trips))
            except Exception as error:
                problems.append("could not summarise the trips: %r" % error)
        if self.readings:
            final = self.readings[-1]
            row["monitor_final_running_mean"] = round(final["total_trip_overhead_average"], 4)
            late = [reading["routing_duration"] for reading in self.readings if reading["step"] > 1000]
            if late:
                row["routing_duration_mean_after_1000"] = round(statistics.mean(late), 3)

        if treatment == "rtx_step":
            for record in knowledge["sweep"]:
                value = record["exploration_percentage"]
                if record["sample_mean"] is not None:
                    row["sample_mean_%s" % value] = round(record["sample_mean"], 4)
                if record["seen_in_configs_at_step"] is not None:
                    row["seen_in_configs_step_%s" % value] = record["seen_in_configs_at_step"]
            row["sweep_finished"] = knowledge["sweep_finished"]
            if knowledge["chosen_exploration"] is not None:
                row["chosen_exploration"] = knowledge["chosen_exploration"]
            if knowledge["best"]["seen_in_configs_at_step"] is not None:
                row["chosen_applied_step"] = knowledge["best"]["seen_in_configs_at_step"]

        row["status"] = "ok" if not problems else "failed: " + "; ".join(problems)

        # raw data of the run, next to the run table
        with open(context.run_dir / "readings.json", "w") as f:
            json.dump(self.readings, f)
        with open(context.run_dir / "strategy_knowledge.json", "w") as f:
            json.dump({"treatment": treatment, "start_step": self.start_step,
                       "trips_before": self.trips_before, "analysis_data": knowledge,
                       "problems": problems}, f, indent=1)
        if trips:
            with open(context.run_dir / "trips.jsonl", "w") as f:
                for trip in trips:
                    f.write(json.dumps(trip) + "\n")

        output.console_log("Config.populate_run_data() called! %s" % row["status"])
        return row

    def after_experiment(self) -> None:
        output.console_log("Config.after_experiment() called!")

    # ================================ DO NOT ALTER BELOW THIS LINE ================================
    experiment_path:            Path             = None
