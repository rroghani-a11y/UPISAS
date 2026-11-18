# UPISAS
Unified Python interface for self-adaptive system exemplars.

### Prerequisites 
Tested with Python 3.9.12, should work with >=3.7.

### Installation
In a terminal, navigate to the parent folder of the project and issue:
```
pip install -r requirements.txt
```
### Run unit tests
In a terminal, navigate to the parent folder of the project and issue:
```
python -m UPISAS.tests.upisas.test_exemplar
python -m UPISAS.tests.upisas.test_strategy
python -m UPISAS.tests.swim.test_swim_interface
```
### Run
In a terminal, navigate to the parent folder of the project and issue:
```
python run.py
```

### Using experiment runner 
**Please be advised**, experiment runner does not work on native Windows. Since UPISAS also uses docker, your Windows system should have the Windows Subsystem for Linux (WSL) installed already. You can then simply use Python within the WSL for both UPISAS and Experiment Runner (restart the installation above from scratch there, and then proceed with the below).
```
cd experiment-runner
git submodule update --init --recursive
pip install -r requirements.txt
cd ..
sh run.sh 
```



### CrowdNav baseline (RTX step strategy)
The baseline of our project is the step strategy of RTX (Schmid et al., SEAMS 2017, Listing 4).
It tries `exploration_percentage` 0.0, 0.1 and 0.2 one after the other, measures the mean trip overhead of each and then keeps the best one.
The strategy is in `UPISAS/strategies/crowdnav_rtx_step.py`. The experiment compares it with a run where nothing is adapted.

Start the CrowdNav containers (kafka, http-server, crowdnav) first, then from the parent folder of the project:
```
python experiment-runner/experiment-runner/ UPISAS/experiment_runner_configs/RunnerConfig_crowdnav_baseline.py
```
This does 10 runs of 10,000 steps (5 per treatment, about 45 minutes in total).
Every run starts from the values in CrowdNav's `knobs.json`. For our results that was `explorationPercentage` 0.5 and `reRouteEveryTicks` 600.
The results go to `UPISAS/experiment_runner_configs/experiments/crowdnav_rtx_step_baseline/`: a `run_table.csv` with one row per run and a folder per run with the monitor readings and the trips.

To use the sample sizes of the paper (ignore 1000 trips, sample 5000, 80,000 steps per run):
```
BASELINE_PAPER_SIZES=1 python experiment-runner/experiment-runner/ UPISAS/experiment_runner_configs/RunnerConfig_crowdnav_baseline.py
```
On macOS a run can crash at its first request. Setting `no_proxy='*'` before the command fixes that.
