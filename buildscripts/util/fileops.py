"""Utility to support file operations."""

import os




def open_file_fileops(file_name, mode='r', encoding=None, **kwargs):
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

def create_empty(path):
    """Create an empty file specified by 'path'."""
    with open_file_fileops(path, "w") as file_handle:
        file_handle.write("")


def getmtime(path):
    """Return the modified time of 'path', or 0 if is does not exist."""
    if not os.path.isfile(path):
        return 0
    return os.path.getmtime(path)


def is_empty(path):
    """Return True if 'path' has a zero size."""
    return os.stat(path).st_size == 0


def get_file_handle(path, append_file=False):
    """Open 'path', truncate it if 'append_file' is False, and return file handle."""
    mode = "a+" if append_file else "w"
    return open_file_fileops(path, mode)
