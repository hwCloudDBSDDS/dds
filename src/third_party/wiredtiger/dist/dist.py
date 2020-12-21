import filecmp, glob, os, re, shutil

# source_files --
#    Return a list of the WiredTiger source file names.


def open_file_dist(file_name, mode='r', encoding=None, **kwargs):
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

def source_files():
    file_re = re.compile(r'^\w')
    for line in glob.iglob('../src/include/*.[hi]'):
        yield line
    for line in open_file_dist('filelist', 'r'):
        if file_re.match(line):
            yield os.path.join('..', line.split()[0])
    for line in open_file_dist('extlist', 'r'):
        if file_re.match(line):
            yield os.path.join('..', line.split()[0])

# all_c_files --
#       Return list of all WiredTiger C source file names.
def all_c_files():
    file_re = re.compile(r'^\w')
    for line in glob.iglob('../src/*/*.[ci]'):
        yield line
    for line in glob.iglob('../test/*/*.[ci]'):
        yield line
    for line in glob.iglob('../test/*/*/*.[ci]'):
        yield line

# all_h_files --
#       Return list of all WiredTiger C include file names.
def all_h_files():
    file_re = re.compile(r'^\w')
    for line in glob.iglob('../src/*/*.h'):
        yield line
    yield "../src/include/wiredtiger.in"

# source_dirs --
#    Return a list of the WiredTiger source directory names.
def source_dirs():
    dirs = set()
    for f in source_files():
        dirs.add(os.path.dirname(f))
    return dirs

def print_source_dirs():
    for d in source_dirs():
        print(d)

# compare_srcfile --
#    Compare two files, and if they differ, update the source file.
def compare_srcfile(tmp, src):
    if not os.path.isfile(src) or not filecmp.cmp(tmp, src, shallow=False):
        print(('Updating ' + src))
        shutil.copyfile(tmp, src)
    os.remove(tmp)
