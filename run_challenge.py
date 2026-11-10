import time

from UPISAS.exemplars.crowdnav import CrowdNav
from UPISAS.strategies.my_strategy import MyStrategy

LAST_STEP = 10000      # one run is 10,000 simulation steps
PAUSE_SECONDS = 3      # time between two rounds of the loop

if __name__ == "__main__":
    exemplar = CrowdNav()
    exemplar.start_run()

    strategy = MyStrategy(exemplar)

    while True:
        # Monitor
        strategy.monitor()
        car_stats = strategy.knowledge.monitored_data["car_stats"][-1]
        print("step", car_stats["step"], "mean trip overhead", car_stats["total_trip_overhead_average"])

        if car_stats["step"] >= LAST_STEP:
            break

        # Analyze, Plan, Execute
        if strategy.analyze():
            if strategy.plan():
                # validation is off because the schema allows edge_average_influence
                # only up to 1, while CrowdNav really runs with 140
                strategy.execute(with_validation=False)

        time.sleep(PAUSE_SECONDS)
