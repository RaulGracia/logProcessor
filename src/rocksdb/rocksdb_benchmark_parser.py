import time

'''Base directory where logs from Pravega experiment are located'''
LOGS_DIR = "resources/results2018-07-04_15-37-32.txt"
BENCHMARK_START_SIGNAL = "results_"
PARAM_VALUE_SEPARATOR = "="

'''Track and record simple statistics from RocksDB benchmark'''
STATISTICS = "STATISTICS:"
STATISTICS_TO_CAPTURE = ["rocksdb.block.cache.miss", "rocksdb.block.cache.hit", "rocksdb.block.cache.bytes.write",
                         "rocksdb.block.cache.bytes.read", "rocksdb.memtable.hit", "rocksdb.memtable.miss"]

'''Track and record histogram info from RocksDB benchmark'''
READ_HISTOGRAM = "rocksdb.db.get.micros P50 :"
HISTOGRAMS_TO_CAPTURE = ["rocksdb.db.get.micros", "rocksdb.db.write.micros", "rocksdb.read.block.get.micros",
                         "rocksdb.write.raw.block.micros", "rocksdb.db.flush.micros"]

'''Keep the interesting values for out analysis'''
benchmark_results = dict()


def add_new_parameter_keys(line):
    line = line[len(BENCHMARK_START_SIGNAL):]
    benchmark_id = 'benchmark'
    while PARAM_VALUE_SEPARATOR in line:
        param = line[:line.index(PARAM_VALUE_SEPARATOR)]
        line = line[line.index(PARAM_VALUE_SEPARATOR)+1:]
        value = line[:line.index("_")]
        line = line[line.index("_")+1:]
        benchmark_id += "_" + param + "_" + value

    benchmark_results[benchmark_id] = dict()
    return benchmark_id


def get_statistic(current_benchmark, line):
    for statistic in STATISTICS_TO_CAPTURE:
        if line.startswith(statistic):
            benchmark_results[current_benchmark][statistic] = line[len(statistic + " COUNT : "):]
            return


def get_histogram(current_benchmark, line):
    for histogram in HISTOGRAMS_TO_CAPTURE:
        if line.startswith(histogram):
            line = line[len(histogram + " P50 : "):]
            benchmark_results[current_benchmark][histogram + "_P50"] = line[:line.index(" ")]
            line = line[line.index(" P99 : ") + len(" P99 : "):]
            benchmark_results[current_benchmark][histogram + "_P99"] = line[:line.index(" ")]
            line = line[line.index(" COUNT : ") + len(" COUNT : "):]
            benchmark_results[current_benchmark][histogram + "_PCOUNT"] = line[:line.index(" ")]
            return


def parse_benchmark_results(log_file_path):
    current_benchmark = None
    read_histogram_flag = False
    statistics_flag = False
    with open(log_file_path, 'r') as lf:
        for line in lf:
            line = line[:-1]
            '''First, initialize the dictionary with per-benchmark results'''
            if line.startswith(BENCHMARK_START_SIGNAL):
                current_benchmark = add_new_parameter_keys(line)
                statistics_flag = False
                continue

            if line.startswith(STATISTICS):
                statistics_flag = True
                continue

            if line.startswith(READ_HISTOGRAM):
                statistics_flag = False
                read_histogram_flag = True

            '''Once the benchmark id is set, store the interesting metrics in its results dictionary'''
            if statistics_flag:
                get_statistic(current_benchmark, line)

            if read_histogram_flag:
                get_histogram(current_benchmark, line)


def print_pretty_output():
    output_file = open("clean_benchmark_results.dat", 'w')
    output_line = ''
    '''Print headers'''
    for benchmark_id in sorted(benchmark_results.keys()):
        output_line += ' ' * len(benchmark_id) + '\t'
        for field in sorted(benchmark_results[benchmark_id].keys()):
            output_line += field + '\t'
        print >> output_file, output_line
        break

    '''Write results'''
    output_line = ''
    for benchmark_id in sorted(benchmark_results.keys()):
        output_line += benchmark_id + '\t'
        for field in sorted(benchmark_results[benchmark_id].keys()):
            output_line += benchmark_results[benchmark_id][field] + '\t'
        print >> output_file, output_line
        output_line = ''

    output_file.close()


if __name__ == "__main__":
    ini_time = time.time()
    parse_benchmark_results(LOGS_DIR)
    print_pretty_output()
    print "Time elapsed (sec.): ", (time.time()-ini_time)