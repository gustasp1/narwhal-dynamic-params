# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError


@task
def local(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': 2_00,
        'tx_size': 512,
        'duration': 10, # seconds
        'fluctuation': True,
        'duty_cycle_duration': 2, # seconds
        'fluc_high_rate': 2_000,
        'fluc_low_rate': 100,
    }   
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 50_000,  # bytes
        'max_batch_delay': 200,  # ms
        'learning': False,
        'quorum_threshold': '2f+1',
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug=debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)

@task
def learnlocal(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 2,
        'workers': 1,
        'tx_size': 512,
        'duration': 2,
        'rate': [1_00],
    }
    node_params = {
        'header_size': [1_002],  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': [13, 5_000],  # bytes
        'max_batch_delay': 200,  # ms
        'quorum_threshold': ['1f'],
        'duration': 2,
        'learning': True,
    }
    try:
        ret = LocalBench(bench_params, node_params).learn()
    except BenchError as e:
        Print.error(e)

@task
def create(ctx, nodes=1):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=False, param_type='dynamic'):
    ''' Run benchmarks on AWS '''
    bench_params = {
        'faults': 3,
        'nodes': [10],
        'workers': 1,
        'collocate': True,
        'rate': [125_000],
        'tx_size': 512,
        'duration': 110,
        'runs': 1,
        'fluctuation': True,
        'duty_cycle_duration': 30, # seconds
        'fluc_high_rate': 40_000,
        'fluc_low_rate': 2_500,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'quorum_threshold': '2f+1',
        'learning': False
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug=debug, param_type=param_type)
    except BenchError as e:
        Print.error(e)

@task
def learnremote(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 10,
        'workers': 1,
        'rate': [1_000],
        'duration': 5,
        'tx_size': 512,
    }
    node_params = {
        'header_size': [1_000],  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': [1],  # bytes
        'max_batch_delay': 200,  # ms
        'quorum_threshold': ['2f+1'],  # 1, f, f+1, or 2f+1
        'learning': True,  # true by default 
    }
    try:
        Bench(ctx).learn(bench_params, node_params, 1, debug)
    except BenchError as e:
        Print.error(e)

@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [10],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [2_000, 10_000]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))

@task
def fplot(ctx):
    ''' Plot input rate, TPS, and latency against time while input rate is flactuating '''
    try:
        Ploter.plot_fluctuations()
    except PlotError as e:
        Print.error(BenchError('Failed to plot flactuations', e))

@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
