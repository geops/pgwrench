#!/usr/bin/python

from distutils.core import setup
from pgwrench import app

setup(name='pgwrench',
  version=app.version,
  packages=['pgwrench'],
  scripts=['pgwrench.py'])
