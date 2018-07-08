import time

'''Simple parsing script for the command nohup top -b | awk '/bash|Swap:|Mem :|Cpu|influx|ssh|java|beam.smp|dockerd/ 
{print systime(), $0}' > top_monitoring.txt &'''

'''Base directory where top monitoring logs are located'''
LOGS_DIR = "resources/top_monitoring_sample.txt"

'''Output files'''
PROCESS_MEMORY_OUTPUT = "process_memory_monitoring.txt"
PROCESS_CPU_OUTPUT = "process_cpu_monitoring.txt"
SYSTEM_OUTPUT = "system_monitoring.txt"

CPU_MARK = '%Cpu(s):'
MEM_MARK = 'KiB Mem :'
SWAP_MARK = 'KiB Swap:'

process_dict = dict()
cpu_results = list()
mem_results = list()


def parse_top_monitoring(log_file_path):
    initial_timestamp = None
    with open(log_file_path, 'r') as lf:
        for line in lf:
            line = ' '.join(line[:-1].split())

            timestamp = long(line[:line.index(" ")])
            if initial_timestamp is None:
                initial_timestamp = timestamp

            if CPU_MARK in line:
                line = line[line.index(CPU_MARK) + len(CPU_MARK):]
                line = line[:line.index("us,")]
                cpu_results.append((timestamp-initial_timestamp, float(line)))
                continue

            if MEM_MARK in line:
                line = line[line.index("free,") + len("free,"):line.index(" used,")]
                mem_results.append((timestamp-initial_timestamp, long(line)))
                continue

            if SWAP_MARK in line:
                continue

            line_splits = line.split(" ")
            if len(line_splits) < 9:
                continue

            process_id = line_splits[1] + "-" + line_splits[2] + "-" + line_splits[-1]

            if process_id not in process_dict:
                process_dict[process_id] = dict()
                process_dict[process_id]['cpu_rel'] = list()
                process_dict[process_id]['mem_rel'] = list()
                process_dict[process_id]['mem_res'] = list()

            process_dict[process_id]['cpu_rel'].append(float(line_splits[9]))
            process_dict[process_id]['mem_rel'].append(float(line_splits[10]))

            mem_reserved = line_splits[6]
            if 'g' in mem_reserved:
                mem_reserved = mem_reserved.replace("g", "")
                mem_reserved = long(float(mem_reserved) * 1024 * 1024 * 1024)
            elif 'm' in mem_reserved:
                mem_reserved = mem_reserved.replace("m", "")
                mem_reserved = long(float(mem_reserved) * 1024 * 1024)
            else: mem_reserved = long(mem_reserved)

            process_dict[process_id]['mem_res'].append(mem_reserved)


def print_pretty_output():
    output_file_proc_mem = open(PROCESS_MEMORY_OUTPUT, 'w')
    output_file_proc_cpu = open(PROCESS_CPU_OUTPUT, 'w')
    output_file_system = open(SYSTEM_OUTPUT, 'w')

    for (t_cpu, t_mem) in zip(cpu_results, mem_results):
        print >> output_file_system, t_cpu[0], t_cpu[1], t_mem[1] / 1024.  # Results in MB of system memory

    for proc_id in sorted(process_dict):
        output_line = proc_id
        for cpu_rel in process_dict[proc_id]['cpu_rel']:
            output_line += ' ' + str(cpu_rel)
        print >> output_file_proc_cpu, output_line

    for proc_id in sorted(process_dict):
        output_line = proc_id
        for cpu_rel in process_dict[proc_id]['mem_rel']:
            output_line += ' ' + str(cpu_rel)
        print >> output_file_proc_mem, output_line
        output_line = proc_id
        for cpu_rel in process_dict[proc_id]['mem_res']:
            output_line += ' ' + str(cpu_rel)
        print >> output_file_proc_mem, output_line

    output_file_proc_mem.close()
    output_file_proc_cpu.close()
    output_file_system.close()




if __name__ == "__main__":
    ini_time = time.time()
    parse_top_monitoring(LOGS_DIR)
    print_pretty_output()
    print "Time elapsed (sec.): ", (time.time()-ini_time)