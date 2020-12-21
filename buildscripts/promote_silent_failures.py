#!/usr/bin/env python
"""Convert silent test failures into non-silent failures.

Any test files with at least 2 executions in the report.json file that have a "silentfail" status,
this script will change the outputted report to have a "fail" status instead.
"""

import collections
import json
import optparse
import os
import sys

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from buildscripts.resmokelib.testing import report




def open_file_promote_silent_failures(file_name, mode='r', encoding=None, **kwargs):
    if mode in ['r', 'rt', 'tr'] and encoding is None:
        with open(file_name, 'rb') as f:
            context = f.read()
            for encoding_item in ['UTF-8', 'GBK', 'ISO-8859-1']:
                try:
                    context.decode(encoding=encoding_item)
                    encoding = encoding_item
                    break
                except UnicodeDecodeError as e:
                    pass
    return open(file_name, mode=mode, encoding=encoding, **kwargs)

def read_json_file(json_file):
    """Return contents of a JSON file."""
    with open_file_promote_silent_failures(json_file) as json_data:
        return json.load(json_data)


def main():
    """Execute Main program."""

    usage = "usage: %prog [options] report.json"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-o", "--output-file", dest="outfile", default="-",
                      help=("If '-', then the report file is written to stdout."
                            " Any other value is treated as the output file name. By default,"
                            " output is written to stdout."))

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("Requires a single report.json file.")

    report_file_json = read_json_file(args[0])
    test_report = report.TestReport.from_dict(report_file_json)

    # Count number of "silentfail" per test file.
    status_dict = collections.defaultdict(int)
    for test_info in test_report.test_infos:
        if test_info.evergreen_status == "silentfail":
            status_dict[test_info.test_id] += 1

    # For test files with more than 1 "silentfail", convert status to "fail".
    for test_info in test_report.test_infos:
        if status_dict[test_info.test_id] >= 2:
            test_info.evergreen_status = "fail"

    result_report = test_report.as_dict()
    if options.outfile != "-":
        with open_file_promote_silent_failures(options.outfile, "w") as fp:
            json.dump(result_report, fp)
    else:
        print((json.dumps(result_report)))


if __name__ == "__main__":
    main()
