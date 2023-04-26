from setuptools import setup, find_packages

setup(
    name='customer_orders',
    version='1.0',
    packages=find_packages(include=('job*',)),
    description='customer_orders',
    install_requires=[
        'pyhocon==0.3.59',
        'prophecy-libs==0.1.6'
    ],
    entry_points={
        'console_scripts': [
            'main = job.pipeline:main',
        ],
    },
    tests_require=['pytest', 'pytest-html']
)
