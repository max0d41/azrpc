from setuptools import setup

setup(
    name='azrpc',
    version='1.0.0',
    url='https://github.com/max0d41/azrpc',
    description='A robust and feature rich RPC system based on ZeroMQ and gevent.',
    packages=[
        'azrpc',
    ],
    install_requires=[
        'zmq',
        'gevent',
        'functionregister',
    ],
    dependency_links=[
        'https://github.com/max0d41/functionregister/archive/master.zip#egg=functionregister-1.0.0',
    ],
)
