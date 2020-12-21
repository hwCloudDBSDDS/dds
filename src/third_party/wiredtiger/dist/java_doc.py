#!/usr/bin/env python

# This program pulls the function names from wiredtiger.in and generates
# an input file for Java SWIG that adds doxygen copydoc comments to functions.

import os, re, sys
import api_data
from dist import compare_srcfile

# Temporary file.
tmp_file = '__tmp'

#####################################################################
# Update wiredtiger.in with doxygen comments
#####################################################################
f='../src/include/wiredtiger.in'
o='../lang/java/java_doc.i'


def open_file_java_doc(file_name, mode='r', encoding=None, **kwargs):
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

tfile = open_file_java_doc(tmp_file, 'w')

tfile.write('''/* DO NOT EDIT: automatically built by dist/java_doc.py. */

''')

cclass_re = re.compile('^struct __([a-z_]*) {')
cfunc_re = re.compile('\t.*? __F\(([a-z_]*)\)')

curr_class = ""
for line in open_file_java_doc(f, 'r'):

    m = cclass_re.match(line)
    if m:
        curr_class = m.group(1)

    if curr_class == "":
        continue

    m = cfunc_re.match(line)
    if m:
        tfile.write('COPYDOC(__' + curr_class.lower() + ', ' +
        curr_class.upper() + ', ' + m.group(1) + ')\n')

tfile.close()
compare_srcfile(tmp_file, o)
