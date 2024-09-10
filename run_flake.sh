#!/bin/sh

flake8 . --count --exclude=./test/resources/,./.venv/ --select=E9,F63,F7,F82 --show-source --statistics
flake8 . --count --exclude=./test/resources/,./.venv/ --statistics
