from setuptools import setup, find_packages
setup(
    name = 'raw_bronze',
    version = '1.0',
    packages = find_packages(include = ('raw_bronze*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.16'],
    entry_points = {
'console_scripts' : [
'main = raw_bronze.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
