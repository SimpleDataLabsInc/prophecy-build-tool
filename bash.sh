
rm -r dist/*
python3.10 setup.py bdist_wheel
pip3.10 install --force-reinstall dist/prophecy_build_tool-*