import click
from benchmarking.benchmarks.throughput import base as throughput_base
from benchmarking.benchmarks.throughput import kafka as throughput_kafka
from benchmarking.benchmarks.ysb import base as ysb_base
from benchmarking.benchmarks.ysb import kafka as ysb_kafka
from benchmarking.benchmarks.ysb.base import GiB

@click.group()
def kafka():
    pass

@kafka.group("ysb")
def kafka_ysb():
    pass

@kafka_ysb.command()
def sustainable():
    def _(*args, **kwargs):
        return ysb_kafka.create_kafka_benchmark(*args, interleaved=True, **kwargs)
    ysb_base.sustainable_throughput_suite(_, data_size=20 * GiB, max_rate_limit=1 * GiB)
 
@kafka_ysb.command()
def isolated():
    def _(*args, **kwargs):
        return ysb_kafka.create_kafka_benchmark(*args, interleaved=False, **kwargs)
    ysb_base.isolated_throughput_suite(_, 20 * GiB)

@kafka.group()
def micro():
    pass

@micro.group("throughput")
def kafka_throughput():
    pass

@kafka_throughput.command()
def sustainable():
    def _(*args, **kwargs):
        return throughput_kafka.create_kafka_benchmark(*args, interleaved=True, **kwargs)
    throughput_base.sustainable_throughput_suite(_, data_size=20 * GiB, max_rate_limit=1 * GiB)
 
@kafka_throughput.command()
def isolated():
    def _(*args, **kwargs):
        return throughput_kafka.create_kafka_benchmark(*args, interleaved=False, **kwargs)
    throughput_base.isolated_throughput_suite(_, 20 * GiB)

@micro.command()
def roundtrip():
    ...

if __name__ == "__main__":
    kafka()
