from UPISAS.strategy import Strategy


class MyStrategy(Strategy):
    """Sets two CrowdNav knobs to the values below when they are different."""

    # The values we want. Each constant has the name of its CrowdNav knob.

    # exploration_percentage: the share of route requests where the router
    # prefers roads without fresh measurements over the fastest known route.
    # 0.5 in the bad setup, so every second request.
    EXPLORATION_PERCENTAGE = 0.0
    # ticks on the road. 600 in the bad setup.
    RE_ROUTE_EVERY_TICKS = 30

    def analyze(self):
        """Are the two knobs in CrowdNav different from the values we want?"""
        configs = self.knowledge.monitored_data["configs"][-1]

        exploration_percentage = configs["exploration_percentage"]
        re_route_every_ticks = configs["re_route_every_ticks"]

        needs_change = (
            exploration_percentage != self.EXPLORATION_PERCENTAGE
            or re_route_every_ticks != self.RE_ROUTE_EVERY_TICKS
        )

        self.knowledge.analysis_data["needs_change"] = needs_change
        return needs_change

    def plan(self):
        """Keep all nine knobs as they are and change only these two."""
        configs = self.knowledge.monitored_data["configs"][-1]

        new_configs = dict(configs)
        new_configs["exploration_percentage"] = self.EXPLORATION_PERCENTAGE
        new_configs["re_route_every_ticks"] = self.RE_ROUTE_EVERY_TICKS

        self.knowledge.plan_data = new_configs
        return True
