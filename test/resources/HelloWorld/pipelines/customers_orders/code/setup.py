from setuptools import setup, find_packages
setup(
    name = 'customers_orders1243',
    version = '0.0.1',
    packages = find_packages(include = ('job*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs>=2.1.10'],
    entry_points = {
'console_scripts' : [
'main = job.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
