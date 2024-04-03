#!/bin/bash


# Path to the requirements.txt file
REQUIREMENTS_FILE="donut-master/requirment.txt"

# Check if requirements.txt exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "requirements.txt not found"
    exit 1
fi

# Install libraries listed in requirements.txt
pip install -r donut-master/requirment.txt

DOWNLOAD_FILE="donut-master/download_training_data.py"

python "$DOWNLOAD_FILE" $1 $2

DATASET_FILE="donut-master/run_donut.py"

python "$DATASET_FILE"

TRAINER_FILE="donut-master/donut_training.py"

python "$TRAINER_FILE"
