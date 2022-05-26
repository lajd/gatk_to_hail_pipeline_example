import os
from setuptools import setup

__NAME__ = 'GATK-to-hail-pipeline-example'
__VERSION__ = '0.1.0'
__DESCRIPTION__ = 'Example pipeline from unmapped reads (BAM) to a hail matrix table'
__AUTHOR__ = 'Jonathan La'


def get_requirements():
    with open(os.path.abspath('./requirements.txt'), 'r') as f:
        requirements = [i.strip() for i in f.readlines()]
    return requirements


setup(
    name=__NAME__,
    version_info=__VERSION__,
    description=__DESCRIPTION__,
    author=__AUTHOR__,
    license='MIT',
    platforms=['any'],
    zip_safe=False,
    python_requires='>=3.7',
    include_package_data=True,
    install_requires=get_requirements(),
    package_data={},
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown'
)
