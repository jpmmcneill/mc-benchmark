## mc-benchmark

To run a benchmark suite, first clone this repo.

Then, ensure poetry is installed in your python. You can use a venv to keep environments isolated if you want to.


`pip install poetry`

Then, run `poetry shell` from the `mc-benchmark` directory.

Run `python bm.py benchmark -s simple_scenario` to run the "hello world" scenario.
This outputs data for this scenario to the `benchmark_results` folder.

Run `python bm.py benchmark -s simple_scenario` to run analysis the "hello world" scenario.
This requires the benchmark_results data to be populated for the given scenario.

Some scenarios take a long time to benchmark (for example: `casino_simulation` takes ~8 hours on an m3 max macbook pro).
Depending on your hardware, it may be worth it to trim down the scenario you run!

### Available commands
```python
python bm.py list
# lists the available scenarios
python bm.py benchmark -s <scenario_name>
# generate benchmark data for a given scenario (can be very slow!!!)
python bm.py analyze -s <scenario_name>
# generate analysis data for a given scenario
```
