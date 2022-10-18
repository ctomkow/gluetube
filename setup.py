# Craig Tomkow
# 2022-08-11

# local imports
import gluetube

# python imports
from setuptools import setup, find_namespace_packages
import os

# read from the VERSION file
with open(os.path.join(os.path.dirname(gluetube.__file__), 'VERSION')) as version_file:
    version = version_file.read().strip()

# Package meta-data.
NAME = 'gluetube'
DESCRIPTION = 'Glue systems together with pipelines'
URL = 'https://github.com/ctomkow/gluetube'
EMAIL = 'ctomkow@gmail.com'
AUTHOR = 'Craig Tomkow'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = version

setup(
        name=NAME,
        version=VERSION,
        description=DESCRIPTION,
        url=URL,
        author=AUTHOR,
        author_email=EMAIL,
        license='MIT',
        install_requires=[
            'python-daemon>=2.3.1,<3.0.0',
            'rocketry>=2.4.1,<3.0.0'
        ],

        entry_points={
            'console_scripts': [
                'gluetube=gluetube.gluetube:Gluetube',
            ],
        },
        packages=find_namespace_packages(where="."),
        package_dir={"": "."},
        package_data={
            'gluetube': ['VERSION'],
            'gluetube.cfg': ['*.cfg'],
        },
    )
