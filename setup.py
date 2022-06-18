import setuptools

with open('README.md', 'r', encoding='utf-8') as _in:
    long_description = _in.read()

setuptools.setup(
    name='prophecy-build-tool',
    version='1.0.0',
    author='Prophecy',
    author_email='maciej@prophecy.io',
    description='Prophecy-build-tool (PBT) provides utilities to build and distribute projects created from the '
                'Prophecy IDE.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/SimpleDataLabsInc/prophecy-build-tool',
    project_urls={
        'Bug Tracker': 'https://github.com/SimpleDataLabsInc/prophecy-build-tool/issues',
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        "License :: OSI Approved :: MIT License",
        'Operating System :: OS Independent',
    ],
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src'),
    install_requires=[
        'requests~=2.28.0',
        'PyYAML~=6.0',
        'databricks_cli==0.17.0'
    ],
    python_requires='>=3.6',
    entry_points = '''
        [console_scripts]
        pbt=pbt:main
    '''
)
