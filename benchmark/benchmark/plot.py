# Copyright(C) Facebook, Inc. and its affiliates.
from collections import defaultdict
from re import findall, search, split
import matplotlib.pyplot as plt
import matplotlib.ticker as tick
from glob import glob
from itertools import cycle

from benchmark.utils import PathMaker
from benchmark.config import PlotParameters
from benchmark.aggregate import LogAggregator

from pathlib import Path
import shutil

@tick.FuncFormatter
def default_major_formatter(x, pos):
    if pos is None:
        return
    if x >= 1_000:
        return f'{x/1000:.0f}k'
    else:
        return f'{x:.0f}'


@tick.FuncFormatter
def sec_major_formatter(x, pos):
    if pos is None:
        return
    return f'{float(x)/1000:.1f}'


@tick.FuncFormatter
def mb_major_formatter(x, pos):
    if pos is None:
        return
    return f'{x:,.0f}'


class PlotError(Exception):
    pass


class Ploter:
    def __init__(self, filenames):
        if not filenames:
            raise PlotError('No data to plot')

        self.results = []
        try:
            for filename in filenames:
                with open(filename, 'r') as f:
                    self.results += [f.read().replace(',', '')]
        except OSError as e:
            raise PlotError(f'Failed to load log files: {e}')

    def _natural_keys(self, text):
        def try_cast(text): return int(text) if text.isdigit() else text
        return [try_cast(c) for c in split('(\d+)', text)]

    def _tps(self, data):
        values = findall(r' TPS: (\d+) \+/- (\d+)', data)
        values = [(int(x), int(y)) for x, y in values]
        return list(zip(*values))

    def _latency(self, data, scale=1):
        values = findall(r' Latency: (\d+) \+/- (\d+)', data)
        values = [(float(x)/scale, float(y)/scale) for x, y in values]
        return list(zip(*values))

    def _variable(self, data):
        return [int(x) for x in findall(r'Variable value: X=(\d+)', data)]

    def _tps2bps(self, x):
        data = self.results[0]
        size = int(search(r'Transaction size: (\d+)', data).group(1))
        return x * size / 10**6

    def _bps2tps(self, x):
        data = self.results[0]
        size = int(search(r'Transaction size: (\d+)', data).group(1))
        return x * 10**6 / size

    def _find_neighbors(self, x, lst):
        if x <= lst[0]:
            return (0, 0)
        for i in range(len(lst)-1):
            if x >= lst[i] and x <= lst[i+1]:
                return (i, i+1)
            
        return (len(lst)-1, len(lst)-1) 
            

    def _plot(self, x_label, y_label, y_axis, z_axis, type):
        plt.figure()
        self.results.sort(key=self._natural_keys, reverse=(type == 'tps'))
        markers = cycle(['o', 'v'])
        colors = cycle(['green', 'green', 'firebrick', 'firebrick'])
        linestyles = cycle(['dashed', 'dotted'])
        if len(self.results) == 2:
            markers = cycle(['o', 'o', 'v', 's', 'p', 'D', 'P'])
            colors = cycle(['green', 'firebrick', 'firebrick'])
            linestyles = cycle(['dashed', 'dashed'])
        static_x = {}
        dynamic_x = {}
        sizes = []
        plot_improvements = False
        for result in self.results:
            y_values, y_err = y_axis(result)
            x_values = self._variable(result)

            label = z_axis(result)
            print(label)
            size = label[10]
            if size not in sizes:
                sizes.append(size)
            if "parameters" in label:
                plot_improvements = True

            if "static" in label:
                static_x[size] = {x_values[i] : y_values[i] for i in range(len(x_values))}
            if "dynamic" in label:
                dynamic_x[size] = {x_values[i] : y_values[i] for i in range(len(x_values))}
            if len(y_values) != len(y_err) or len(y_err) != len(x_values):
                raise PlotError('Unequal number of x, y, and y_err values')

            plt.errorbar(
                x_values, y_values, yerr=y_err, label=label,
                linestyle=next(linestyles), marker=next(markers), capsize=3, color = next(colors)
            )

        improvements_x = defaultdict(list)
        improvements_y = defaultdict(list)
        for label in sizes:
            print(label)
            if len(static_x) == 0:
                continue
            if static_x[label] or dynamic_x[label]: 
                static_key_list = sorted(static_x[label].keys())

                for x in dynamic_x[label].keys():
                    l, r = self._find_neighbors(x, static_key_list)
                    if l is None:
                        continue
                    l = static_key_list[l]
                    r = static_key_list[r]
                    
                    x_diff = r - l
                    if x_diff == 0:
                        x_diff = 1
                    y_diff = static_x[label][r] - static_x[label][l]

                    val = static_x[label][l] + y_diff / x_diff * (x - l)
                    improvement = (dynamic_x[label][x] - val) / val * 100 * -1


                    improvements_x[label].append(x)
                    improvements_y[label].append(improvement)



        plt.legend(loc='lower center', bbox_to_anchor=(0.5, 1), ncol=2)
        plt.xlim(xmin=0)
        # plt.xlim(xmin=0)
        plt.ylim(0, 3600)
        plt.xlabel(x_label, fontweight='bold')
        plt.ylabel(y_label[0], fontweight='bold')
        plt.xticks(weight='bold')
        plt.yticks(weight='bold')
        plt.grid()
        ax = plt.gca()
        ax.xaxis.set_major_formatter(default_major_formatter)
        ax.yaxis.set_major_formatter(default_major_formatter)
        ax.figure.set_size_inches(7.6, 3)
        if 'latency' in type:
            ax.yaxis.set_major_formatter(sec_major_formatter)
        if len(y_label) > 1:
            secaxy = ax.secondary_yaxis(
                'right', functions=(self._tps2bps, self._bps2tps)
            )
            secaxy.set_ylabel(y_label[1])
            secaxy.yaxis.set_major_formatter(mb_major_formatter)

        for x in ['pdf', 'png']:
            plt.savefig(PathMaker.plot_file(type, x), bbox_inches='tight')

        if not plot_improvements:
            return
        plt.clf()

        markers = cycle(['o', 'v', 's', 'p', 'D', 'P'])
        colors = cycle(['firebrick', 'mediumblue', 'green'])

        for label in sizes:


        # plt.plot(improvements_x, )
            plt.errorbar(
                    improvements_x[label], improvements_y[label], label=f"10 nodes ({label} faulty)",
                    linestyle='dashed', marker=next(markers), capsize=3, color = next(colors)
                )

            plt.legend(loc='lower center', bbox_to_anchor=(0.83, 0.67))
            plt.xlim(xmin=0, xmax=20_000)
            plt.ylim(-30, 50)
            plt.xlabel("Throughput (tx/s)", fontweight='bold')
            plt.ylabel("Performance Gain (%)", fontweight='bold')
            plt.xticks(weight='bold')
            plt.yticks(weight='bold')
            plt.grid()
            ax.axhline(0, color='black', linewidth=1, label='_no_legend_', zorder=3, linestyle='dotted')
            ax = plt.gca()
            ax.xaxis.set_major_formatter(default_major_formatter)
            ax.yaxis.set_major_formatter(default_major_formatter)
            ax.figure.set_size_inches(7.6, 3)

            plt.savefig("plots/improvements.pdf", bbox_inches='tight')

    @staticmethod
    def nodes(data):
        x = search(r'Committee size: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        param_type = search(r'Parameter type: (.*)', data)
        if param_type:
            param_type = param_type.group(1)
        faults = f' ({f} faulty)'
        # faults = f' ({f} faulty)' if f != '0' else ''
        params = f', {param_type} parameters' if param_type else ''
        return f'{x} nodes{faults}{params}'

    @staticmethod
    def workers(data):
        x = search(r'Workers per node: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f'({f} faulty)' if f != '0' else ''
        return f'{x} workers {faults}'

    @staticmethod
    def max_latency(data):
        x = search(r'Max latency: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f'({f} faulty)' if f != '0' else ''
        return f'Max latency: {float(x) / 1000:,.1f} s {faults}'

    @classmethod
    def plot_latency(cls, files, scalability):
        assert isinstance(files, list)
        assert all(isinstance(x, str) for x in files)
        z_axis = cls.workers if scalability else cls.nodes
        x_label = 'Throughput (tx/s)'
        y_label = ['Latency (s)']
        ploter = cls(files)
        ploter._plot(x_label, y_label, ploter._latency, z_axis, 'latency')

    @classmethod
    def plot_tps(cls, files, scalability):
        assert isinstance(files, list)
        assert all(isinstance(x, str) for x in files)
        z_axis = cls.max_latency
        x_label = 'Workers per node' if scalability else 'Committee size'
        y_label = ['Throughput (tx/s)', 'Throughput (MB/s)']
        ploter = cls(files)
        ploter._plot(x_label, y_label, ploter._tps, z_axis, 'tps')

    @classmethod
    def plot(cls, params_dict):
        try:
            params = PlotParameters(params_dict)
        except PlotError as e:
            raise PlotError('Invalid nodes or bench parameters', e)

        # Aggregate the logs.
        LogAggregator(params.max_latency).print()

        # Make the latency, tps, and robustness graphs.
        iterator = params.workers if params.scalability() else params.nodes
        latency_files, tps_files = [], []
        for f in params.faults:
            for x in iterator:
                for param_type in ['static', 'dynamic']:
                    latency_files += glob(
                        PathMaker.agg_file(
                            'latency',
                            f,
                            x if not params.scalability() else params.nodes[0],
                            x if params.scalability() else params.workers[0],
                            params.collocate,
                            'any',
                            params.tx_size,
                            param_type
                        )
                    )

            for l in params.max_latency:
                for param_type in ['static', 'dynamic']:
                    tps_files += glob(
                        PathMaker.agg_file(
                            'tps',
                            f,
                            'x' if not params.scalability() else params.nodes[0],
                            'x' if params.scalability() else params.workers[0],
                            params.collocate,
                            'any',
                            params.tx_size,
                            param_type,
                            max_latency=l
                        )
                    )

        cls.plot_latency(latency_files, params.scalability())
        cls.plot_tps(tps_files, params.scalability())

    @classmethod
    def plot_fluctuations(cls):
        try:
            if Path("fluctuation_plots").exists():
                shutil.rmtree('fluctuation_plots')
            Path("fluctuation_plots").mkdir()
            x_label = "Time (s)"
            y_label = ["Throughput", "Latency (s)"]
            
            with open('input_rates.txt', 'r') as f:
                lines = f.readlines()
                min_time = min(max([float(t) for t in lines[1][1:len(lines[1])-2].split(', ')]), \
                          max([float(t) for t in lines[3][1:len(lines[3])-2].split(', ')]))
                index = 2
                fix, axs = plt.subplots(2, 1, figsize=(4, 5))
                for i, filename in enumerate(['tps_vs_time.pdf', 'latency_vs_time.pdf']):
                    values = [float(value) for value in lines[index][1:len(lines[index])-2].split(', ')]
                    index += 1
                    times = [float(t) for t in lines[index][1:len(lines[index])-2].split(', ')]
                    index += 1
                    print(filename)

                    axs[i].plot(times, values)
                    axs[i].set_xlabel(x_label, fontweight='bold')
                    axs[i].set_ylabel(y_label[i], fontweight='bold')
                    # axs[i].set_xticks(weight='bold')
                    # axs[i].set_yticks(weight='bold')
                    axs[i].grid(True)
                    # ax = plt.gca()
                    # ax.figure.set_size_inches(9, 2.6)
                    if i == 1:
                        axs[i].set_ylim(bottom = 0, top=3.5)
                    else:
                        axs[i].set_ylim(bottom = 0, top=None)

                    axs[i].set_xlim(left = 0, right=120)

                    # axs[i].set_xticks(weight='bold')
                    # axs[i].set_yticks(weight='bold')

                    axs[i].xaxis.set_major_formatter(default_major_formatter)
                    axs[i].yaxis.set_major_formatter(default_major_formatter)

                    # axs[i].tick_params(axis='both', which='major', fontweight='bold')
                
                    
                    # plt.savefig(f"fluctuation_plots/{filename}", bbox_inches='tight')
                    # plt.clf()

                plt.tight_layout()
                plt.savefig(f"fluctuation_plots/all.pdf", bbox_inches='tight')

        except FileNotFoundError as e:
            raise PlotError('Input rates file could not be found', e)