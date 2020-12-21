#!/usr/bin/env python
"""Distutils installer for extras."""

from setuptools import setup
import os.path

import extras
testtools_cmd = extras.try_import('testtools.TestCommand')




def open_file_setup(file_name, mode='r', encoding=None, **kwargs):
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

def get_version():
    """Return the version of extras that we are building."""
    version = '.'.join(
        str(component) for component in extras.__version__[0:3])
    return version


def get_long_description():
    readme_path = os.path.join(
        os.path.dirname(__file__), 'README.rst')
    return open_file_setup(readme_path).read()


cmdclass = {}

if testtools_cmd is not None:
    cmdclass['test'] = testtools_cmd


setup(name='extras',
      author='Testing cabal',
      author_email='testtools-dev@lists.launchpad.net',
      url='https://github.com/testing-cabal/extras',
      description=('Useful extra bits for Python - things that shold be '
        'in the standard library'),
      long_description=get_long_description(),
      version=get_version(),
      classifiers=["License :: OSI Approved :: MIT License"],
      packages=[
        'extras',
        'extras.tests',
        ],
      cmdclass=cmdclass)
