import time

'''Simple parsing script for the command nohup iotop -b -P -o -k -d 3 | awk '/bash|Total|Actual|PID|influx|ssh|java|beam.smp|mesos|dockerd/ {print systime(), $0}' | cut -c -220 > iotop_monitoring.txt &'''

'''Measurement periodicity'''
PERIODICITY = 3

'''Base directory where top monitoring logs are located'''
LOGS_DIR = "resources/iotop_monitoring.txt"

'''Output files'''
SYSTEM_IO_OUTPUT = "system_io_monitoring.txt"
PROCESS_DISK_READ_OUTPUT = "process_disk_read_monitoring.txt"
PROCESS_DISK_WRITE_OUTPUT = "process_disk_write_monitoring.txt"
PROCESS_SWAPING_OUTPUT = "process_swaping_monitoring.txt"
PROCESS_IO_OUTPUT = "process_io_monitoring.txt"

TOTAL_MARK = 'Total'
ACTUAL_MARK = 'Actual'
PID_MARK = '  PID:'

process_dict = dict()
io_total_write_results = list()
io_total_read_results = list()
io_actual_write_results = list()
io_actual_read_results = list()


def parse_iotop_monitoring(log_file_path):
    initial_timestamp = None
    with open(log_file_path, 'r') as lf:
        for line in lf:
            line = ' '.join(line[:-1].split())
            if '%' in line:
                line = line[0:line.rfind('%')] + line[line.rfind('%')+1:-1].replace(" ", "").replace("\"", "")
            line_splits = line.split(" ")

            if len(line_splits) < 12 or "PID" in line:
                continue

            timestamp = long(line_splits[0])
            if initial_timestamp is None:
                initial_timestamp = timestamp

            if TOTAL_MARK in line_splits[1]:
                '''All sizes are in kilobytes'''
                io_total_read_results.append((timestamp-initial_timestamp, float(line_splits[5])))
                io_total_write_results.append((timestamp-initial_timestamp, float(line_splits[12])))
                continue

            if ACTUAL_MARK in line_splits[1]:
                '''All sizes are in kilobytes'''
                io_actual_read_results.append((timestamp-initial_timestamp, float(line_splits[4])))
                io_actual_write_results.append((timestamp-initial_timestamp, float(line_splits[10])))
                continue

            '''Per process headers PID  PRIO  USER     DISK READ  DISK WRITE  SWAPIN      IO    COMMAND'''
            process_id = line_splits[1] + "-" + line_splits[-1]

            if process_id not in process_dict:
                process_dict[process_id] = dict()
                process_dict[process_id]['disk_read'] = list()
                process_dict[process_id]['disk_write'] = list()
                process_dict[process_id]['swaping'] = list()
                process_dict[process_id]['io'] = list()

            process_dict[process_id]['disk_read'].append((timestamp-initial_timestamp, float(line_splits[4])))
            process_dict[process_id]['disk_write'].append((timestamp-initial_timestamp, float(line_splits[6])))
            process_dict[process_id]['swaping'].append((timestamp-initial_timestamp, float(line_splits[8])))
            process_dict[process_id]['io'].append((timestamp-initial_timestamp, float(line_splits[10])))


def output_per_process_metric(output_file, metric_name, experiment_length):
    for proc_id in sorted(process_dict):
        ini_values = [0] * int(experiment_length/PERIODICITY + 1)
        for value_tuple in process_dict[proc_id][metric_name]:
            ini_values[value_tuple[0]/PERIODICITY] = str(value_tuple[1])

        '''Remove periodic zero values that come from non-perfect periodicity of iotop command'''
        for i in range(2, len(ini_values)):
            if ini_values[i-1] == 0 and ini_values[i] != 0:
                ini_values[i-1] = ini_values[i]

        print >> output_file, proc_id + '\t' + str(ini_values).replace("[", "").replace("]", "").replace(", ", "\t").replace("\'", "")


def print_pretty_output():
    output_file_system = open(SYSTEM_IO_OUTPUT, 'w')
    output_file_proc_disk_read = open(PROCESS_DISK_READ_OUTPUT, 'w')
    output_file_proc_disk_write = open(PROCESS_DISK_WRITE_OUTPUT, 'w')
    output_file_proc_swaping= open(PROCESS_SWAPING_OUTPUT, 'w')
    output_file_proc_io = open(PROCESS_IO_OUTPUT, 'w')

    print >> output_file_system, "TIME TOTAL_READ TOTAL_WRITE ACTUAL_READ ACTUAL_WRITE"
    for (t_total_r, t_total_w, t_actual_r, t_actual_w) in zip(io_total_read_results, io_total_write_results, io_actual_read_results, io_actual_write_results):
        print >> output_file_system, t_total_w[0], t_total_r[1] / 1024., t_total_w[1] / 1024., t_actual_r[1] / 1024., t_actual_w[1] / 1024.  # Results in MB of system memory

    '''Delete short-lived processes and the the maximum living process'''
    experiment_length = None
    for proc_id in sorted(process_dict):
        if len(process_dict[proc_id]['disk_read']) < 1:
            del process_dict[proc_id]
        else:
            for t_io in process_dict[proc_id]['disk_read']:
                if experiment_length is None or t_io[0] > experiment_length:
                    experiment_length = t_io[0]

    timeline = ''
    for j in range(0, experiment_length, PERIODICITY):
        timeline += '\t' + str(j)

    print >> output_file_proc_disk_read, timeline
    output_per_process_metric(output_file_proc_disk_read, 'disk_read', experiment_length)
    print >> output_file_proc_disk_write, timeline
    output_per_process_metric(output_file_proc_disk_write, 'disk_write', experiment_length)
    print >> output_file_proc_swaping, timeline
    output_per_process_metric(output_file_proc_swaping, 'swaping', experiment_length)
    print >> output_file_proc_io, timeline
    output_per_process_metric(output_file_proc_io, 'io', experiment_length)

    output_file_system.close()
    output_file_proc_disk_read.close()
    output_file_proc_disk_write.close()
    output_file_proc_swaping.close()
    output_file_proc_io.close()


if __name__ == "__main__":
    ini_time = time.time()
    parse_iotop_monitoring(LOGS_DIR)
    print_pretty_output()
    print "Time elapsed (sec.): ", (time.time() - ini_time)
