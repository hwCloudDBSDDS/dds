"""Pseudo-builders for building and registering unit tests.
"""
from SCons.Script import Action



def open_file_mongo_unittest(file_name, mode='r', encoding=None, **kwargs):
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

def exists(env):
    return True

_unittests = []
def register_unit_test(env, test):
    _unittests.append(test.path)
    env.Alias('$UNITTEST_ALIAS', test)

def unit_test_list_builder_action(env, target, source):
    ofile = open_file_mongo_unittest(str(target[0]), 'wb')
    try:
        for s in _unittests:
            print(('\t' + str(s)))
            ofile.write('%s\n' % s)
    finally:
        ofile.close()

def build_cpp_unit_test(env, target, source, **kwargs):
    libdeps = kwargs.get('LIBDEPS', [])
    libdeps.append( '$BUILD_DIR/mongo/unittest/unittest_main' )

    kwargs['LIBDEPS'] = libdeps
    kwargs['INSTALL_ALIAS'] = ['tests']

    result = env.Program(target, source, **kwargs)
    env.RegisterUnitTest(result[0])
    hygienic = env.GetOption('install-mode') == 'hygienic'
    if not hygienic:
        env.Install("#/build/unittests/", result[0])
    return result

def generate(env):
    env.Command('$UNITTEST_LIST', env.Value(_unittests),
            Action(unit_test_list_builder_action, "Generating $TARGET"))
    env.AddMethod(register_unit_test, 'RegisterUnitTest')
    env.AddMethod(build_cpp_unit_test, 'CppUnitTest')
    env.Alias('$UNITTEST_ALIAS', '$UNITTEST_LIST')
