# Workaround
# http://bugs.python.org/issue15881
try:
    import multiprocessing
except:
    pass

from setuptools import setup, find_packages

setup(
    name         = 'exporters',
    version      = '0.1',
    packages     = find_packages(exclude=['tests']),
    test_suite   = 'tests',
    tests_require = ['mock', 'moto', 'coverage'],
)
