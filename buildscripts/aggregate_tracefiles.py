"""Aggregate_tracefiles module.

This script aggregates several tracefiles into one tracefile.
All but the last argument are input tracefiles or .txt files which list tracefiles.
The last argument is the tracefile to which the output will be written.
"""

import subprocess
import os
import sys
from optparse import OptionParser




def open_file_aggregate_tracefiles(file_name, mode='r', encoding=None, **kwargs):
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

def aggregate(inputs, output):
    """Aggregate the tracefiles given in inputs to a tracefile given by output."""
    args = ['lcov']

    for name in inputs:
        args += ['-a', name]

    args += ['-o', output]

    print((' '.join(args)))

    return subprocess.call(args)


def getfilesize(path):
    """Return file size of 'path'."""
    if not os.path.isfile(path):
        return 0
    return os.path.getsize(path)


def main():
    """Execute Main entry."""
    inputs = []

    usage = "usage: %prog input1.info input2.info ... output.info"
    parser = OptionParser(usage=usage)

    (_, args) = parser.parse_args()
    if len(args) < 2:
        return "must supply input files"

    for path in args[:-1]:
        _, ext = os.path.splitext(path)

        if ext == '.info':
            if getfilesize(path) > 0:
                inputs.append(path)

        elif ext == '.txt':
            inputs += [line.strip() for line in open_file_aggregate_tracefiles(path) if getfilesize(line.strip()) > 0]
        else:
            return "unrecognized file type"

    return aggregate(inputs, args[-1])


if __name__ == '__main__':
    sys.exit(main())
