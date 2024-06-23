
from pathlib import Path

import pandas as pd
import tenacity

from benchmarking.benchmarks.base import Benchmark, Consumer, Producer
from benchmarking.deployment import create_log_path

class ThroughputBenchmark(Benchmark):
    pass

class ThroughputProducer(Producer):
    pass

class ThroughputConsumer(Consumer):
    pass

KB = 1000
MB = 1000 * KB
GB = 1000 * MB


KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB


def sustainable_throughput_suite(create_benchmark, data_size, max_rate_limit) -> None:
    max_batch_size = 1 * MiB
    min_batch_size = 1 * KiB
    results = []
    batch_size = max_batch_size
    base_log_path = create_log_path("throughput")
    
    while batch_size >= min_batch_size:
        tp = find_sustainable_throughput(batch_size, data_size, max_rate_limit, base_log_path, create_benchmark)
        results.append((batch_size, tp))
        max_rate_limit = int(tp * 1.5)
        data_size = min(data_size, 90 * tp)
        batch_size //= 2
    print(results)    

def isolated_throughput_suite(create_benchmark, data_size) -> None:
    max_batch_size = 1 * MiB
    min_batch_size = 1 * KiB
    batch_size = max_batch_size
    base_log_path = create_log_path("throughput")
    
    while batch_size >= min_batch_size:
        data_size = data_size if batch_size >= 32 * KiB else data_size // 2
        benchmark = create_benchmark(data_size, data_size, batch_size, base_log_path)
        benchmark.run()
        batch_size //= 2
    print("Isolated throughput suite done")    

def find_sustainable_throughput(batch_size: int, data_size: int, max_rate_limit: int, base_log_path: Path, create_benchmark) -> int:
    left = 0 * MiB
    right = max_rate_limit

    difference = right - left
    while difference > (right * 0.02) and difference > 2 * MiB:
        rate_limit = (left + right) // 2
        tries = 0
        while tries < 3:
            tries += 1
            is_sustainable = try_single_run(rate_limit, data_size, batch_size, base_log_path, create_benchmark)
            if is_sustainable is True:
                left = rate_limit
                break
            elif is_sustainable is False:
                right = rate_limit
                break
        if is_sustainable is None:
            right = rate_limit
        difference = right - left
    return left + (difference // 2)

@tenacity.retry(stop=tenacity.stop_after_attempt(2), wait=tenacity.wait_exponential(16))
def try_single_run(rate_limit: int, data_size, batch_size, base_log_path, create_benchmark) -> bool:
    benchmark = create_benchmark(rate_limit, data_size, batch_size, base_log_path)
    
    benchmark.run()
    df_producer_throughput = pd.read_csv(benchmark._log_path / "benchmark_producer_throughput.csv", sep="\t")
    median_producer_throughput = df_producer_throughput["Throughput in MiB/s"].median() * MiB
    producer_sustainable = median_producer_throughput > 0.99 * rate_limit
    print(f"Producer Throughput: {median_producer_throughput // MiB} of {rate_limit // MiB} ({median_producer_throughput / rate_limit})")
    df_consumer_throughput = pd.read_csv(benchmark._log_path / "benchmark_consumer_throughput.csv", sep="\t")
    median_consumer_throughput = df_consumer_throughput["Throughput in MiB/s"].median() * MiB
    consumer_sustainable = (median_consumer_throughput > 0.99 * rate_limit)
    print(f"Consumer Throughput: {median_consumer_throughput // MiB} of {rate_limit // MiB} ({median_consumer_throughput / rate_limit})")
    sustainable = None
    if producer_sustainable and consumer_sustainable:
        sustainable = True
    # Neither is within 90% of rate limit
    elif (max(median_producer_throughput, median_consumer_throughput) / rate_limit) < 0.90:
        sustainable = False
    # Throughputs diverge by >20%
    elif (abs(median_producer_throughput - median_consumer_throughput) / min(median_producer_throughput, median_consumer_throughput)) > 0.2:
        sustainable = False
    if sustainable:
        (benchmark._log_path / "sustainable.flag").touch()
    return sustainable
