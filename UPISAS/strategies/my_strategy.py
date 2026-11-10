from UPISAS.strategy import Strategy


class MyStrategy(Strategy):
    """Notices the bad router configuration and fixes it once."""

    # the values we want CrowdNav to run with
    GOOD_EXPLORATION = 0.2
    GOOD_REROUTE = 60

    def analyze(self):
        """Is CrowdNav running with a bad configuration right now?"""
        configs = self.knowledge.monitored_data["configs"][-1]

        explores_too_much = configs["exploration_percentage"] != self.GOOD_EXPLORATION
        reroutes_too_rarely = configs["re_route_every_ticks"] != self.GOOD_REROUTE
        is_bad = explores_too_much or reroutes_too_rarely

        self.knowledge.analysis_data["bad_config"] = is_bad
        return is_bad

    def plan(self):
        """Keep all nine knobs as they are and change only the two bad ones."""
        configs = self.knowledge.monitored_data["configs"][-1]

        new_configs = dict(configs)
        new_configs["exploration_percentage"] = self.GOOD_EXPLORATION
        new_configs["re_route_every_ticks"] = self.GOOD_REROUTE

        

        self.knowledge.plan_data = new_configs
        return True
