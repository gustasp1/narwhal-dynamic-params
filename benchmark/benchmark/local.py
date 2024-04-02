# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class LocalBench:
    BASE_PORT = 3000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters_dict = bench_parameters_dict
            self.node_parameters_dict = node_parameters_dict
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True, errors=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)
        
    def learn(self):
        best_latency = {}
        rates = self.bench_parameters.rate
        batch_sizes = self.node_parameters.json['batch_size']
        header_sizes = self.node_parameters.json['header_size']
        quorum_thresholds = self.node_parameters.json['quorum_threshold']

        total_runs = len(rates) * len(batch_sizes) * len(header_sizes) * len(quorum_thresholds)
        counter = 1
        self.node_parameters_dict["quorum_threshold"] = True

        for input_rate in rates:
            for batch_size in batch_sizes:
                for header_size in header_sizes:
                    for quorum_threshold in quorum_thresholds:
                        best_latency[input_rate] = (float('inf'), 0, 0, 0)
                        Print.info(f"Running phase {counter} / {total_runs}")
                        counter += 1
                        self.bench_parameters_dict["input_rate"] = input_rate
                        self.node_parameters_dict["batch_size"] = batch_size
                        self.node_parameters_dict["header_size"] = header_size
                        self.node_parameters_dict["quorum_threshold"] = quorum_threshold
                        self.bench_parameters = BenchParameters(self.bench_parameters_dict)
                        self.node_parameters = NodeParameters(self.node_parameters_dict)
                        self.run(learning=True, debug=True)

                        latency = LogParser.process(PathMaker.logs_path(), faults=self.faults)._end_to_end_latency()
                        if latency > 0 and latency < best_latency[input_rate][0]:
                            best_latency[input_rate] = (latency, batch_size, header_size, quorum_threshold)

        with open("system_level_config.txt", "w") as f:
            for input_rate, (_, batch_size, header_size, quorum_threshold) in best_latency.items():
                f.write(f"{input_rate},{batch_size},{header_size},{quorum_threshold}\n")


    def run(self, learning=True, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            nodes, rate = self.nodes[0], self.rate[0]
            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT, self.workers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Run the clients (they will wait for the nodes to be ready).
            workers_addresses = committee.workers_addresses(self.faults)
            rate_share = ceil(rate / committee.workers())
            low_rate_share = ceil(self.fluc_low_rate / committee.workers())
            high_rate_share = ceil(self.fluc_high_rate / committee.workers())
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    cmd = CommandMaker.run_client(
                        address,
                        self.tx_size,
                        self.fluctuation,
                        self.duty_cycle_duration,
                        rate_share,
                        low_rate_share,
                        high_rate_share,
                        [x for y in workers_addresses for _, x in y]
                    )
                    log_file = PathMaker.client_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Run the primaries (except the faulty ones).
            for i, address in enumerate(committee.primary_addresses(self.faults)):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    debug=debug
                )
                log_file = PathMaker.primary_log_file(i)
                self._background_run(cmd, log_file)

            # Run the workers (except the faulty ones).
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        debug=debug
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process(PathMaker.logs_path(), faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)