# The step strategy of RTX (Schmid et al., SEAMS 2017, Listing 4) as a UPISAS strategy.
# For each exploration value: send it, skip the first trips after the change,
# then take the mean overhead of the next trips.
from UPISAS.strategy import Strategy


# one tenth of the sizes in the paper's Listing 4 (1000 and 5000), so the sweep fits in 10,000 ticks
SCALED = {"first_samples_to_ignore": 100, "sample_size": 500}


def step_values(lower, upper, step):
    # same loop as RTX uses (StepStrategy.py)
    values = []
    value = lower
    while value <= upper:
        values.append(value)
        value += step
    return values


def mean_of_new_trips(trips_before, average_before, trips_after, average_after):
    """Mean overhead of the trips that finished between two monitor readings."""
    if trips_after <= trips_before:
        return None
    # CrowdNav computes its average as sum / (n + 1), so average * (n + 1) gives the sum back
    sum_before = average_before * (trips_before + 1)
    sum_after = average_after * (trips_after + 1)
    return (sum_after - sum_before) / (trips_after - trips_before)


class RtxStepStrategy(Strategy):
    """Tries each exploration_percentage value in turn and measures its mean trip overhead."""

    # Listing 4: "knobs": {"exploration_percentage": ([0.0, 0.3], 0.1)}
    EXPLORATION_BOUNDS = [0.0, 0.3]
    EXPLORATION_STEP = 0.1
    EXPLORATION_VALUES = step_values(EXPLORATION_BOUNDS[0], EXPLORATION_BOUNDS[1], EXPLORATION_STEP)

    def __init__(self, exemplar, parameters=SCALED):
        super().__init__(exemplar)
        self.IGNORE_FIRST_TRIPS = parameters["first_samples_to_ignore"]
        self.SAMPLE_TRIPS = parameters["sample_size"]

        # phase goes send -> ignore -> sample, and done at the end
        state = self.knowledge.analysis_data
        state["phase"] = "send"
        state["value_index"] = 0
        state["target"] = self.EXPLORATION_VALUES[0]
        state["sweep"] = [self._new_record(value) for value in self.EXPLORATION_VALUES]
        state["sweep_finished"] = False

    @staticmethod
    def _new_record(value):
        return {
            "exploration_percentage": value,
            "sent_at_step": None,
            "ignore_until_trips": None,
            "sample_start_step": None,
            "sample_start_trips": None,
            "sample_start_average": None,
            "sample_end_step": None,
            "sample_end_trips": None,
            "sample_end_average": None,
            "sample_trips": None,
            "sample_mean": None,
        }

    def analyze(self):
        """Moves the sweep one step. True when a value has to be sent."""
        state = self.knowledge.analysis_data
        car_stats = self.knowledge.monitored_data["car_stats"][-1]
        step = car_stats["step"]
        trips = car_stats["total_trips"]
        average = car_stats["total_trip_overhead_average"]
        record = state["sweep"][state["value_index"]]

        if state["phase"] == "ignore":
            if trips >= record["ignore_until_trips"]:
                record["sample_start_step"] = step
                record["sample_start_trips"] = trips
                record["sample_start_average"] = average
                state["phase"] = "sample"
        elif state["phase"] == "sample":
            if trips >= record["sample_start_trips"] + self.SAMPLE_TRIPS:
                record["sample_end_step"] = step
                record["sample_end_trips"] = trips
                record["sample_end_average"] = average
                record["sample_trips"] = trips - record["sample_start_trips"]
                record["sample_mean"] = mean_of_new_trips(
                    record["sample_start_trips"], record["sample_start_average"], trips, average)
                self._go_to_next_value()

        return state["phase"] == "send"

    def _go_to_next_value(self):
        state = self.knowledge.analysis_data
        if state["value_index"] + 1 < len(self.EXPLORATION_VALUES):
            state["value_index"] += 1
            state["target"] = self.EXPLORATION_VALUES[state["value_index"]]
            state["phase"] = "send"
            return
        state["sweep_finished"] = True
        state["phase"] = "done"

    def plan(self):
        """Keeps all nine knobs as they are and changes only exploration_percentage."""
        state = self.knowledge.analysis_data
        configs = self.knowledge.monitored_data["configs"][-1]
        car_stats = self.knowledge.monitored_data["car_stats"][-1]

        new_configs = dict(configs)
        new_configs["exploration_percentage"] = state["target"]
        self.knowledge.plan_data = new_configs

        record = state["sweep"][state["value_index"]]
        record["sent_at_step"] = car_stats["step"]
        record["ignore_until_trips"] = car_stats["total_trips"] + self.IGNORE_FIRST_TRIPS
        state["phase"] = "ignore"
        return True
