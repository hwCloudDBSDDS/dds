"""Manage interactions with the report.json file."""

import json

from . import config
from .testing import report as _report




def open_file_reportfile(file_name, mode='r', encoding=None, **kwargs):
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

def write(suites):
    """Write the combined report of all executions if --reportFile was specified."""

    if config.REPORT_FILE is None:
        return

    reports = []
    for suite in suites:
        reports.extend(suite.get_reports())

    combined_report_dict = _report.TestReport.combine(*reports).as_dict()
    with open_file_reportfile(config.REPORT_FILE, "w") as fp:
        json.dump(combined_report_dict, fp)
