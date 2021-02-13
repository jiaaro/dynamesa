#!/usr/bin/env zsh

# Note that a ~/.pypirc must exist to run this to completion

python setup.py sdist
python setup.py bdist_wheel
twine check dist/*
if [[ "$?" == "0" ]]; then
	twine upload dist/*
fi