from setuptools import setup, find_packages
from subprocess import Popen, PIPE


def _git_version():
    p = Popen(['git', 'describe', '--always'], stdout=PIPE)
    d = p.communicate()[0].strip('\n')
    p = Popen(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], stdout=PIPE)
    b = p.communicate()[0].strip('\n')
    return '%s-%s' % (d, b)

scripts = [
    'bin/export.py',
]


setup(
    name='exporters',
    version=_git_version(),
    packages=find_packages(),
    scripts=scripts
)
