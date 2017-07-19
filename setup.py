#!/usr/bin/env python
# Copyright (C) 2017 Sur Herrera Paredes

# Should use setuptools instead of distutils!!
from distutils.core import setup

setup(name='sutilspy',
      version='0.0.1',
      description='Python Distribution Utilities',
      author='Sur Herrera Paredes',
      author_email='sur00mx@gmail.com',
      url='https://www.python.org/sigs/distutils-sig/',
      packages=['distutils', 'distutils.command'],
     )