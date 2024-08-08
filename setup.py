# -*- coding: utf-8 -*-
# @author: elson Yan
# @file: setup.py
# @time: 2024/7/28 17:11

from setuptools import setup, find_packages

setup(
    name='elson',
    version='0.0.1',
    author="elson Yan",
    author_email="yansc_1996@163.com",
    description="A sample python application",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[],
    install_requires=[
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    zip_safe=False,
)
