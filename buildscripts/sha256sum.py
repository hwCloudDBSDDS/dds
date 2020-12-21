#!/usr/bin/env python2
"""
Computes a SHA256 sum of a file.

Accepts a file path, prints the hex encoded hash to stdout.
"""


import sys
from hashlib import sha256



def open_file_sha256sum(file_name, mode='r', encoding=None, **kwargs):
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

with open_file_sha256sum(sys.argv[1], 'rb') as fh:
    print((sha256(fh.read()).hexdigest()))
