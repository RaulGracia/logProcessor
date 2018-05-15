# Log Processor for Pravega System Tests

This script is a utility for ease the debugging of Pravega system tests. The idea of this script is to generate a single
file that contains summaries of error messages from the test driver log, the controller log and the segment store log in
parallel, thus helping the developer to understand the sequence of errors across multiple subsystems. For example:

---------------------------------
System test 0
---------------------------------
test   controller    segmentstore
  .        .              .
  .      error1           .
error2     .              .
  .        .            error3
  .        .              .

In the script, the time frame granularity is 1 second by default. Note that multiple errors may be displayed in a single
time frame.

Moreover, the script also takes care of extracting all the log files which are partitioned and compressed.

## How To

The script is simple to use:

- Download all the *.tar.gz files from the Jenkins build you want to debug and store them in `my_log_dir`.
- Modify the constant `LOGS_DIR` in the script to point to `my_log_dir`.
- Execute `python processor.py`.

# Customization

There are several parameter that can be customized in this script:

- `MAX_PREVIOUS_LINES`: Lines of logs to be included in the error summary that precede the actual error (good for getting
some context about what is going on).
- `MAX_TRACE_LINES`: Length of the error trace to be included in the error summary.
- `ERROR_KEYWORDS_TO_CATCH`: The script looks for one of the keywords in this list to label a line as the start of an
error to be included in the final output.
- `OUTPUT_FILE`: Name of the output file to be generated.
