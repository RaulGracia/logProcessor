
import gzip
import os, tarfile
import shutil
import re

import datetime
import time

'''Base directory where logs from Pravega experiment are located'''
LOGS_DIR = "path_to_the_dir_with_build_logs"
MAX_PREVIOUS_LINES = 2
MAX_TRACE_LINES = 5
ERROR_KEYWORDS_TO_CATCH = ["exception", "Exception", "failure", " error ", "error:", "error)"]
OUTPUT_FILE = LOGS_DIR + 'errors_timeline.log'

error_traces = dict()
error_traces['testjob'] = dict()
error_traces['controller'] = dict()
error_traces['segmentstore'] = dict()
testlog_time_intervals = dict()

MAX_COLUMN_SIZE = 60
COLUMN_SEPARATOR = '\t'
BLANK_LINE = ' ' * MAX_COLUMN_SIZE
MAX_COLUMNS_PER_ERROR = 10


def extract(tar_url, extract_path='.'):
    print tar_url
    tar = tarfile.open(tar_url, 'r')
    for item in tar:
        tar.extract(item, extract_path)
        if item.name.find(".tgz") != -1 or item.name.find(".tar") != -1:
            extract(item.name, "./" + item.name[:item.name.rfind('/')])
        elif item.name.endswith('.log') and 'testjob' in item.name:
            test_id = item.name.split('/')[9]
            print LOGS_DIR + test_id[0:test_id.index('.')-1] + '.log'
            shutil.move(LOGS_DIR + item.name, LOGS_DIR + test_id[0:test_id.index('.')-1] + '.log')
        elif item.name.find(".gz") != -1:
            try:
                with gzip.open(LOGS_DIR + item.name, 'rb') as f_in:
                    with open(LOGS_DIR + item.name[:-3], 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            except IOError:
                print "IO Error extracting ", item.name


def check_error(log_line):
    for keyword in ERROR_KEYWORDS_TO_CATCH:
        if keyword in log_line:
            return True
    return False


def process_log(log_file_path, target_log):
    print log_file_path
    previous_lines = list()
    processing_error = False
    error_trace = ''
    date_pattern = re.compile(r'(\d+-\d+-\d+\s+\d+:\d+:\d+,\d+)')
    current_timestamp = -1
    previous_datetime = None
    testjob_ini_timestamp = long(-1)
    testjob_end_timestamp = long(-1)
    error_lines = 0
    print "processing: ", log_file_path
    if 'multicontroller' in log_file_path:
        print "stop here"
    with open(log_file_path, 'r') as lf:
        for line in lf:
            '''Keep some lines before an error to better see the cause'''
            previous_lines.append(line)
            if len(previous_lines) > MAX_PREVIOUS_LINES:
                previous_lines = previous_lines[1:]

            '''Check if we are on a regular log line (i.e., starts with date like 2018-04-24 20:37:02,584)'''
            regular_line = len(line) > 24 and date_pattern.match(line) is not None
            if regular_line and line[:19] != previous_datetime:
                current_timestamp = long(time.mktime(datetime.datetime.strptime(line[0:line.index(',')], "%Y-%m-%d %H:%M:%S").timetuple()))
                previous_datetime = line[:19]

            if current_timestamp != -1:
                testjob_end_timestamp = current_timestamp
                if testjob_ini_timestamp == -1:
                    testjob_ini_timestamp = current_timestamp

            '''Check if this line matches any of the errors we want to find'''
            if not processing_error:
                processing_error = check_error(line)
            else: processing_error = not regular_line

            if processing_error:
                '''If we found an error to capture, add first the previous lines to the stacktrace'''
                if len(error_trace) == 0:
                    for prev_line in previous_lines:
                        error_trace = ''.join([error_trace, prev_line])
                '''Then, start appending the trace of the current error'''
                error_trace = ''.join([error_trace, line.replace('\t', '')])
                error_lines += 1
            else:
                if len(error_trace) > 0:
                    if current_timestamp not in error_traces[target_log]:
                        error_traces[target_log][current_timestamp] = ''
                    error_trace = error_trace.split('\n')
                    total_error_lines = len(error_trace)
                    error_trace = error_trace[:MAX_TRACE_LINES]
                    for the_trace in error_trace:
                        error_traces[target_log][current_timestamp] = ''.join([error_traces[target_log][current_timestamp], the_trace])
                    error_traces[target_log][current_timestamp] = ''.join([error_traces[target_log][current_timestamp],
                                                                           "<EndOfMessageSummary (out of " + str(total_error_lines) + " log lines)>"])
                    error_trace = ''
                    error_lines = 0

    if target_log == 'testjob':
        if testjob_ini_timestamp in testlog_time_intervals:
            raise RuntimeError("Two different errors with the same initial timestamp!" + log_file_path[log_file_path.rfind('/')+1:] + \
                               testlog_time_intervals[testjob_ini_timestamp])
        testlog_time_intervals[testjob_ini_timestamp] = (log_file_path[log_file_path.rfind('/')+1:], testjob_end_timestamp)
        print testlog_time_intervals


def initialize_error_trace(target_log, timestamp):
    if timestamp in error_traces[target_log]:
        return error_traces[target_log][timestamp]
    return ''


def build_output_line(output_line, error_trace):
    if error_trace != '':
        output_line = ''.join([output_line, error_trace[:MAX_COLUMN_SIZE], COLUMN_SEPARATOR])
        error_trace = error_trace[MAX_COLUMN_SIZE:]
    else: output_line = ''.join([output_line, BLANK_LINE, COLUMN_SEPARATOR])
    return output_line, error_trace


def pretty_log_errors_output():
    output_error_log = open(OUTPUT_FILE, 'w')
    empty_line = ''.join([' ' * (MAX_COLUMN_SIZE/2-1), '.', ' ' * (MAX_COLUMN_SIZE/2)])
    empty_line = empty_line + COLUMN_SEPARATOR + empty_line + COLUMN_SEPARATOR + empty_line

    for testjob_ini_time in sorted(testlog_time_intervals.keys()):
        (testjob_name, testjob_end_time) = testlog_time_intervals[testjob_ini_time]
        '''Denote start of test job'''
        print >> output_error_log, '-' * ((MAX_COLUMN_SIZE * 3) + 8)
        print >> output_error_log, testjob_name
        print >> output_error_log, '-' * ((MAX_COLUMN_SIZE * 3) + 8)
        print >> output_error_log, ''.join([' ' * (MAX_COLUMN_SIZE/2-4), 'testjob', ' ' * (MAX_COLUMN_SIZE/2),
                                            ' ' * (MAX_COLUMN_SIZE/2-5), 'controller', ' ' * (MAX_COLUMN_SIZE/2),
                                            ' ' * (MAX_COLUMN_SIZE/2-6), 'segmentstore', ' ' * (MAX_COLUMN_SIZE/2)])
        print >> output_error_log, empty_line

        '''Print in a clear manner all the events of the 3 log sources per second'''
        for timestamp in range(testjob_ini_time, testjob_end_time):
            testjob_error_trace = initialize_error_trace('testjob', timestamp)
            controller_error_trace = initialize_error_trace('controller', timestamp)
            segmentstore_error_trace = initialize_error_trace('segmentstore', timestamp)
            finished = testjob_error_trace == '' and controller_error_trace == '' and segmentstore_error_trace == ''
            '''If there are no errors, just print an empty line'''
            if finished:
                print >> output_error_log, empty_line
                continue
            '''If there are errors, print them respecting the format'''
            while not finished:
                output_line, testjob_error_trace = build_output_line('', testjob_error_trace)
                output_line, controller_error_trace = build_output_line(output_line, controller_error_trace)
                output_line, segmentstore_error_trace = build_output_line(output_line, segmentstore_error_trace)
                output_line = output_line.replace('\n', '')

                print >> output_error_log, output_line
                finished = testjob_error_trace == '' and controller_error_trace == '' and segmentstore_error_trace == ''

    output_error_log.close()


if __name__ == "__main__":
    ini_time = time.time()

    '''First, extract all logs from dirs recursively'''
    print 'Start extracting dirs.'
    for f in os.listdir(LOGS_DIR):
        extract(LOGS_DIR + f, LOGS_DIR)
    print 'Done extracting dirs.'
    '''Remove the system test uncompressed dirs due to their excessive size for Windows'''
    shutil.rmtree(LOGS_DIR + 'var/')

    '''Second, add a sequence number to the logs based on the initial instant in the time-lapse they represent'''
    print "Processing logs..."
    for dirpath, dirs, files in os.walk(LOGS_DIR):
        log_files = (lf for lf in files if lf.endswith('.log'))
        for log_file in log_files:
            print "Processing: " + log_file
            if 'testjob' in log_file: process_log(dirpath + "/" + log_file, 'testjob')
            elif 'controller' in log_file: process_log(dirpath + "/" + log_file, 'controller')
            elif 'segmentstore' in log_file: process_log(dirpath + "/" + log_file, 'segmentstore')
            else: print "WARNING: No processing for log file: ", log_file

    print "Done with processing logs..."

    '''Third, output the sequence of events in appropriate format for debug'''
    print "Generating output..."
    pretty_log_errors_output()
    print "Done with generating output..."

    print "Num errors in testjob", len(error_traces['testjob'])
    print "Num errors in controller", len(error_traces['controller'])
    print "Num errors in segmentstore", len(error_traces['segmentstore'])
    print "Time elapsed (sec.): ", (time.time()-ini_time)
