# Parameter Optimization

This repository provides a dynamic parameter optimization for [Narwhal and Tusk](https://arxiv.org/pdf/2105.11827.pdf). The project is cloned from [here](https://github.com/asonnino/narwhal).

# Running Benchmarks

This document explains how to benchmark the codebase and read benchmarks' results. It also provides a step-by-step tutorial to run benchmarks on [Amazon Web Services (AWS)](https://aws.amazon.com) accross multiple data centers (WAN). Note that the benchmarks have to be executed from `benchmark` directory (use `cd benchmark`).

## Local Benchmarks

When running benchmarks, the codebase is automatically compiled with the feature flag `benchmark`. This enables the node to print some special log entries that are then read by the python scripts and used to compute performance. These special log entries are clearly indicated with comments in the code: make sure to not alter them (otherwise the benchmark scripts will fail to interpret the logs).

### Parametrize the benchmark

After cloning the repo and [installing all dependencies](https://github.com/asonnino/narwhal#quick-start), you can use [Fabric](http://www.fabfile.org/) to run benchmarks on your local machine. Locate the task called `local` in the file [fabfile.py](https://github.com/asonnino/narwhal/blob/master/benchmark/fabfile.py):

```python
@task
def local(ctx):
    ...
```

The task specifies two types of parameters, the _benchmark parameters_ and the _nodes parameters_. The benchmark parameters look as follows:

```python
bench_params = {
    'faults': 0,
    'nodes': 4,
    'workers': 1,
    'rate': 1000,
    'tx_size': 512,
    'duration': 10, # seconds
    'fluctuation': True,
    'duty_cycle_duration': 2, # seconds
    'fluc_high_rate': 2_000,
    'fluc_low_rate': 100,
}
```

They specify the number of primaries (`nodes`) and workers per primary (`workers`) to deploy, the input rate (tx/s) at which the clients submits transactions to the system (`rate`), the size of each transaction in bytes (`tx_size`), the number of faulty nodes ('faults), and the duration of the benchmark in seconds (`duration`). The minimum transaction size is 9 bytes, this ensure that the transactions of a client are all different. The benchmarking script will deploy as many clients as workers and divide the input rate equally amongst each client. For instance, if you configure the testbed with 4 nodes, 1 worker per node, and an input rate of 1,000 tx/s (as in the example above), the scripts will deploy 4 clients each submitting transactions to one node at a rate of 250 tx/s. When the parameters `faults` is set to `f > 0`, the last `f` nodes and clients are not booted; the system will thus run with `n-f` nodes (and `n-f` clients). if `fluctuation` is True, the benchmark client will simulate fluctuating input load, and switch between low and high input rates after `duty_cycle_duration`. The low and high input rates of fluctuation are specified by `fluc_low_rate` and `fluc_high_rate`, respectively.

The nodes parameters determine the configuration for the primaries and workers:

```python
node_params = {
    'header_size': 1_000,
    'max_header_delay': 100,
    'gc_depth': 50,
    'sync_retry_delay': 10_000,
    'sync_retry_nodes': 3,
    'batch_size': 500_000,
    'max_batch_delay': 100,
    'quorum_threshold': '2f+1',
    'learning': False
}
```

They are defined as follows:

- `header_size`: The preferred header size. The primary creates a new header when it has enough parents and enough batches' digests to reach `header_size`. Denominated in bytes.
- `max_header_delay`: The maximum delay that the primary waits between generating two headers, even if the header did not reach `max_header_size`. Denominated in ms.
- `gc_depth`: The depth of the garbage collection (Denominated in number of rounds).
- `sync_retry_delay`: The delay after which the synchronizer retries to send sync requests. Denominated in ms.
- `sync_retry_nodes`: Determine with how many nodes to sync when re-trying to send sync-request. These nodes are picked at random from the committee.
- `batch_size`: The preferred batch size. The workers seal a batch of transactions when it reaches this size. Denominated in bytes.
- `max_batch_delay`: The delay after which the workers seal a batch of transactions, even if `max_batch_size` is not reached. Denominated in ms.
- `quorum_threshold`: The quorum waiter threshold specifies how many workers will the quorum waiter wait for before sending the batch to processor. Available choices are '1', 'f', 'f+1', and '2f+1'.
- `learning`: If `learning` is True, parameters will not be dynamically adjusted. If False, they will be.

### Run the benchmark

Once you specified both `bench_params` and `node_params` as desired, run:

```
$ fab local
```

This command first recompiles your code in `release` mode (and with the `benchmark` feature flag activated), thus ensuring you always benchmark the latest version of your code. This may take a long time the first time you run it. It then generates the configuration files and keys for each node, and runs the benchmarks with the specified parameters. It finally parses the logs and displays a summary of the execution similarly to the one below. All the configuration and key files are hidden JSON files; i.e., their name starts with a dot (`.`), such as `.committee.json`.

```
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Faults: 0 node(s)
 Committee size: 4 node(s)
 Worker(s) per node: 1 worker(s)
 Collocate primary and workers: True
 Input rate: 50,000 tx/s
 Transaction size: 512 B
 Execution time: 19 s

 Header size: 1,000 B
 Max header delay: 100 ms
 GC depth: 50 round(s)
 Sync retry delay: 10,000 ms
 Sync retry nodes: 3 node(s)
 batch size: 500,000 B
 Max batch delay: 100 ms

 + RESULTS:
 Consensus TPS: 46,478 tx/s
 Consensus BPS: 23,796,531 B/s
 Consensus latency: 464 ms

 End-to-end TPS: 46,149 tx/s
 End-to-end BPS: 23,628,541 B/s
 End-to-end latency: 557 ms
-----------------------------------------
```

The 'Consensus TPS' and 'Consensus latency' respectively report the average throughput and latency without considering the client. The consensus latency thus refers to the time elapsed between the block's creation and its commit. In contrast, 'End-to-end TPS' and 'End-to-end latency' report the performance of the whole system, starting from when the client submits the transaction. The end-to-end latency is often called 'client-perceived latency'. To accurately measure this value without degrading performance, the client periodically submits 'sample' transactions that are tracked across all the modules until they get committed into a block; the benchmark scripts use sample transactions to estimate the end-to-end latency.

### Run the learning phase

To execute the learning phase locally, run:

```
$ fab learnlocal
```

This command will execute the parameter learning, using the values specified in `bench_params` and `node_params`.

```
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
```

The values of `rate`, `header_size`, `batch_size`, and `quorum_threshold` can be specified as lists. The program will iterate through all combinations of these values, and find the parameters with lowest latency for each input rate. The results will be saved in `benchmark/system_level_config.txt` file, which will be uploaded to each node when executing `fab local` or `fab remote`.

## AWS Benchmarks

This repo integrates various python scripts to deploy and benchmark the codebase on [Amazon Web Services (AWS)](https://aws.amazon.com). They are particularly useful to run benchmarks in the WAN, across multiple data centers. This section provides a step-by-step tutorial explaining how to use them.

### Step 1. Set up your AWS credentials

Set up your AWS credentials to enable programmatic access to your account from your local machine. These credentials will authorize your machine to create, delete, and edit instances on your AWS account programmatically. First of all, [find your 'access key id' and 'secret access key'](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds). Then, create a file `~/.aws/credentials` with the following content:

```
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

Do not specify any AWS region in that file as the python scripts will allow you to handle multiple regions programmatically.

### Step 2. Add your SSH public key to your AWS account

You must now [add your SSH public key to your AWS account](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). This operation is manual (AWS exposes little APIs to manipulate keys) and needs to be repeated for each AWS region that you plan to use. Upon importing your key, AWS requires you to choose a 'name' for your key; ensure you set the same name on all AWS regions. This SSH key will be used by the python scripts to execute commands and upload/download files to your AWS instances.
If you don't have an SSH key, you can create one using [ssh-keygen](https://www.ssh.com/ssh/keygen/):

```
$ ssh-keygen -f ~/.ssh/aws
```

### Step 3. Configure the testbed

The file [settings.json](https://github.com/asonnino/narwhal/blob/master/benchmark/settings.json) (located in [narwhal/benchmarks](https://github.com/asonnino/narwhal/blob/master/benchmark)) contains all the configuration parameters of the testbed to deploy. Its content looks as follows:

```json
{
  "key": {
    "name": "aws",
    "path": "/absolute/key/path"
  },
  "port": 5000,
  "repo": {
    "name": "narwhal",
    "url": "https://github.com/asonnino/narwhal.git",
    "branch": "master"
  },
  "instances": {
    "type": "m5d.8xlarge",
    "regions": [
      "us-east-1",
      "eu-north-1",
      "ap-southeast-2",
      "us-west-1",
      "ap-northeast-1"
    ]
  }
}
```

The first block (`key`) contains information regarding your SSH key:

```json
"key": {
    "name": "aws",
    "path": "/absolute/key/path"
},
```

Enter the name of your SSH key; this is the name you specified in the AWS web console in step 2. Also, enter the absolute path of your SSH private key (using a relative path won't work).

The second block (`ports`) specifies the TCP ports to use:

```json
"port": 5000,
```

Narwhal requires a number of TCP ports, depening on the number of workers per node, Each primary requires 2 ports (one to receive messages from other primaties and one to receive messages from its workers), and each worker requires 3 ports (one to receive client transactions, one to receive messages from its primary, and one to receive messages from other workers). Note that the script will open a large port range (5000-7000) to the WAN on all your AWS instances.

The third block (`repo`) contains the information regarding the repository's name, the URL of the repo, and the branch containing the code to deploy:

```json
"repo": {
    "name": "narwhal",
    "url": "https://github.com/asonnino/narwhal.git",
    "branch": "master"
},
```

Remember to update the `url` field to the name of your repo. Modifying the branch name is particularly useful when testing new functionalities without having to checkout the code locally.

The the last block (`instances`) specifies the [AWS instance type](https://aws.amazon.com/ec2/instance-types) and the [AWS regions](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions) to use:

```json
"instances": {
    "type": "m5d.8xlarge",
    "regions": ["us-east-1", "eu-north-1", "ap-southeast-2", "us-west-1", "ap-northeast-1"]
}
```

The instance type selects the hardware on which to deploy the testbed. For example, `m5d.8xlarge` instances come with 32 vCPUs (16 physical cores), 128 GB of RAM, and guarantee 10 Gbps of bandwidth. The python scripts will configure each instance with 300 GB of SSD hard drive. The `regions` field specifies the data centers to use. If you require more nodes than data centers, the python scripts will distribute the nodes as equally as possible amongst the data centers. All machines run a fresh install of Ubuntu Server 20.04.

### Step 4. Create a testbed

The AWS instances are orchestrated with [Fabric](http://www.fabfile.org) from the file [fabfile.py](https://github.com/asonnino/narwhal/blob/master/benchmark/fabfile.pyy) (located in [narwhal/benchmarks](https://github.com/asonnino/narwhal/blob/master/benchmark)); you can list all possible commands as follows:

```
$ cd narwhal/benchmark
$ fab --list
```

The command `fab create` creates new AWS instances; open [fabfile.py](https://github.com/asonnino/narwhal/blob/master/benchmark/fabfile.py) and locate the `create` task:

```python
@task
def create(ctx, nodes=2):
    ...
```

The parameter `nodes` determines how many instances to create in _each_ AWS region. That is, if you specified 5 AWS regions as in the example of step 3, setting `nodes=2` will creates a total of 10 machines:

```
$ fab create

Creating 10 instances |██████████████████████████████| 100.0%
Waiting for all instances to boot...
Successfully created 10 new instances
```

You can then clone the repo and install rust on the remote instances with `fab install`:

```
$ fab install

Installing rust and cloning the repo...
Initialized testbed of 10 nodes
```

This may take a long time as the command will first update all instances.
The commands `fab stop` and `fab start` respectively stop and start the testbed without destroying it (it is good practice to stop the testbed when not in use as AWS can be quite expensive); and `fab destroy` terminates all instances and destroys the testbed. Note that, depending on the instance types, AWS instances may take up to several minutes to fully start or stop. The command `fab info` displays a nice summary of all available machines and information to manually connect to them (for debug).

### Step 5. Run a benchmark

After setting up the testbed, running a benchmark on AWS is similar to running it locally (see [Run Local Benchmarks](https://github.com/asonnino/narwhal/tree/master/benchmark#local-benchmarks)). Locate the task `remote` in [fabfile.py](https://github.com/asonnino/narwhal/blob/master/benchmark/fabfile.py):

```python
@task
def remote(ctx, debug=False, param_type='dynamic'):
    ...
```

The benchmark parameters are similar to [local benchmarks](https://github.com/asonnino/narwhal/tree/master/benchmark#local-benchmarks) but allow to specify the number of nodes and the input rate as arrays to automate multiple benchmarks with a single command. The parameter `runs` specifies the number of times to repeat each benchmark (to later compute the average and stdev of the results), and the parameter `collocate` specifies whether to collocate all the node's workers and the primary on the same machine. If `collocate` is set to `False`, the script will run one node per data center (AWS region), with its primary and each of its worker running on a dedicated instance.

```python
bench_params = {
    'nodes': [10, 20, 30],
    'workers: 2,
    'collocate': True,
    'rate': [20_000, 30_000, 40_000],
    'tx_size': 512,
    'faults': 0,
    'duration': 300,
    'runs': 2,
    'fluctuation': True,
    'duty_cycle_duration': 30, # seconds
    'fluc_high_rate': 40_000,
    'fluc_low_rate': 2_500,
}
```

Similarly to local benchmarks, the scripts will deploy as many clients as workers and divide the input rate equally amongst each client. Each client is colocated with a worker, and only submit transactions to the worker with whom they share the machine. Note that to run experiments with fluctuating input rate, `param_type` has to be set to 'fluctuating'.

Once you specified both `bench_params` and `node_params` as desired, run:

```
$ fab remote
```

This command first updates all machines with the latest commit of the GitHub repo and branch specified in your file [settings.json](https://github.com/asonnino/narwhal/blob/master/benchmark/settings.json) (step 3); this ensures that benchmarks are always run with the latest version of the code. It then generates and uploads the configuration files to each machine, runs the benchmarks with the specified parameters, and downloads the logs. It finally parses the logs and prints the results into a folder called `results` (which is automatically created if it doesn't already exists). You can run `fab remote` multiple times without fearing to override previous results, the command either appends new results to a file containing existing ones or prints them in separate files. If anything goes wrong during a benchmark, you can always stop it by running `fab kill`.

## Running remote learning

Remote learning can be executed using the same parameters as specified in the "Running learning locally" section. To execute remote learning, run :

```
$ fab learnremote
```

### Step 6. Plot the results

Once you have enough results, you can aggregate and plot them:

```
$ fab plot
```

This command creates a latency graph, a throughput graph, and a robustness graph in a folder called `plots` (which is automatically created if it doesn't already exists). You can adjust the plot parameters to filter which curves to add to the plot:

```python
plot_params = {
    'faults': [0],
    'nodes': [10, 20, 50],
    'workers': [1],
    'collocate': True,
    'tx_size': 512,
    'max_latency': [3_500, 4_500]
}
```

The first graph ('latency') plots the latency versus the throughput. It shows that the latency is low until a fairly neat threshold after which it drastically increases. Determining this threshold is crucial to understand the limits of the system.

Another challenge is comparing apples-to-apples between different deployments of the system. The challenge here is again that latency and throughput are interdependent, as a result a throughput/number of nodes chart could be tricky to produce fairly. The way to do it is to define a maximum latency and measure the throughput at this point instead of simply pushing every system to its peak throughput (where latency is meaningless). The second graph ('tps') plots the maximum achievable throughput under a maximum latency for different numbers of nodes.
