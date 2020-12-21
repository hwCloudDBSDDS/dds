#!/usr/bin/env python
#
# Public Domain 2014-2018 MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# test_bug007.py
#       Regression tests.

import wiredtiger, wttest

# Check that forced salvage works correctly.


def open_file_test_bug007(file_name, mode='r', encoding=None, **kwargs):
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

class test_bug007(wttest.WiredTigerTestCase):
    def test_bug007(self):
        # This is a btree layer test, test files only.
        uri = 'file:test_bug007'

        # Create the object.
        self.session.create(uri, 'value_format=S,key_format=S')
        cursor = self.session.open_cursor(uri, None)
        cursor.close()

        # Force is required if a file doesn't have a reasonable header.
        # Overwrite the file with random data.
        f = open_file_test_bug007('test_bug007', 'w')
        f.write('random data' * 100)
        f.close()

        # Salvage should fail.
        self.assertRaisesWithMessage(
            wiredtiger.WiredTigerError,
            lambda: self.session.salvage(uri), "/WT_SESSION.salvage/")

        # Forced salvage should succeed.
        self.session.salvage(uri, "force")

if __name__ == '__main__':
    wttest.run()
