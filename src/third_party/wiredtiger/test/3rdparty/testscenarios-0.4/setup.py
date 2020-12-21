#!/usr/bin/env python

from setuptools import setup
import os.path



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

description = open_file_setup(os.path.join(os.path.dirname(__file__), 'README'), 'rt').read()

setup(name="testscenarios",
      version="0.4",
      description="Testscenarios, a pyunit extension for dependency injection",
      long_description=description,
      maintainer="Robert Collins",
      maintainer_email="robertc@robertcollins.net",
      url="https://launchpad.net/testscenarios",
      packages=['testscenarios', 'testscenarios.tests'],
      package_dir = {'':'lib'},
      classifiers = [
          'Development Status :: 6 - Mature',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: BSD License',
          'License :: OSI Approved :: Apache Software License',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Topic :: Software Development :: Quality Assurance',
          'Topic :: Software Development :: Testing',
          ],
      install_requires = [
        'testtools',
        ]
      )
