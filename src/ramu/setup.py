#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [ ]

test_requirements = ['pytest>=3', ]

setup(
    author="Bertty Contreras-Rojas",
    author_email='bertty@databloom.ai',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Ramu is a system that provide a pipeline for clasify the sourface quality",
    entry_points={
        'console_scripts': [
            'ramu=ramu.cli:main',
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='ramu',
    name='ramu',
    packages=find_packages(include=['ramu', 'ramu.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/berttty/dsp-2022',
    version='0.1.0',
    zip_safe=False,
)
