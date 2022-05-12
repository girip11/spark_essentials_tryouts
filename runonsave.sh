#!/bin/bash
FILE=$1

# shellcheck source=/dev/null
source ".venv/bin/activate"

echo "Running autoflake"
python -m autoflake -i --remove-all-unused-imports --remove-unused-variable "$FILE"

echo "Running isort"
python -m isort --profile black "$FILE"

#echo "Running pyupgrade"
#python -m pyupgrade --py37-plus "$FILE"
