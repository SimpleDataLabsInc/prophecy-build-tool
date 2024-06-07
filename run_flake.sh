#!/bin/sh

flake8 --exclude=.venv/ . --count --select=E9,F63,F7,F82 --show-source --statistics
