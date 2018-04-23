#!/usr/bin/env python3

from distutils.core import setup
import os.path

SETUP_DIR = os.path.dirname(os.path.realpath(__file__))
SA_DIR = os.path.join(SETUP_DIR, 'sa')

setup(
    name='sa',
    version='1.0',
    description='Stock Analyzer Utilites',
    author='Nico',
    author_email='nico@nwestbury.com',
    packages=['sa', 'sa.tools'],
    package_dir={'sa': SA_DIR},
)
