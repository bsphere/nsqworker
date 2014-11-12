from setuptools import setup

setup(
    name='nsqworker',
    packages=['nsqworker'],
    version='0.0.1',
    install_requires=['tornado', 'pynsq', 'futures'],
)
