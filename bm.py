import click

from mc_benchmark.benchmark import Benchmark
from mc_benchmark.analysis import run_analysis

@click.group()
def cli():
    pass

@cli.command()
@click.option('-s', '--scenario', required=True)
def benchmark(scenario):
    b = Benchmark.from_yaml(scenario)
    b.process_scenarios()

@cli.command()
@click.option('-s', '--scenario', required=True)
def analyze(scenario):
    run_analysis(scenario)

if __name__ == '__main__':
    cli()
