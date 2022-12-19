# Craig Tomkow
# 2022-08-11

# local imports
import gluetube

# python imports
from setuptools import setup, find_namespace_packages
import os
from codecs import open

# read from the VERSION file
with open(os.path.join(os.path.dirname(gluetube.__file__), 'VERSION')) as version_file:
    version = version_file.read().strip()

# long description as readme
with open("README.md", "r", "utf-8") as f:
    readme = f.read()

# Package meta-data.
NAME = 'gluetube'
DESCRIPTION = 'A lightweight python script scheduler'
URL = 'https://github.com/ctomkow/gluetube'
EMAIL = 'ctomkow@gmail.com'
AUTHOR = 'Craig Tomkow'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = version

setup(
        name=NAME,
        version=VERSION,
        description=DESCRIPTION,
        long_description=readme,
        long_description_content_type="text/markdown",
        url=URL,
        author=AUTHOR,
        author_email=EMAIL,
        license='MIT',
        classifiers=[
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
            'Programming Language :: Python :: 3.11',
        ],
        install_requires=[
            'python-daemon>=2.3.1,<3.0.0',
            'apscheduler>=3.9.1,<4.0.0',
            'prettytable==2.5.0',
            'jinja2==3.0.3',
            'cryptography>=38.0.4,<39.0.0',
        ],

        entry_points={
            'console_scripts': [
                'gluetube=gluetube.gluetube:Gluetube',
                'gt=gluetube.gluetube:Gluetube',
            ],
        },
        packages=find_namespace_packages(where="."),
        package_dir={"": "."},
        package_data={
            'gluetube': ['VERSION'],
            'gluetube.cfg': ['*.cfg'],
        },
    )
