#!/bin/bash

# Path to the requirements.txt file
REQUIREMENTS_FILE="donut-master/requirment.txt"

# Check if requirements.txt exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "requirements.txt not found"
    exit 1
fi

# Install libraries listed in requirements.txt
pip install -r "$REQUIREMENTS_FILE"

arg4=$4

# Conditional statements to check the value of the 4th argument
if [ "$arg4" == "donut_invoice" ]; then
    # Run specific scripts for 'donut-invoice'
    DOWNLOAD_FILE_INVOICE="donut-master/download_training_data.py"
    python "$DOWNLOAD_FILE_INVOICE" "$1" "$2" "$3"
elif [ "$arg4" == "donut_dwg" ]; then
    # Run specific scripts for 'donut-dwg'
    GENERATE_FILE_DWG="donut-master/generate_data_for_dwg.py"
    python "$GENERATE_FILE_DWG" "$1" "$2"
    DOWNLOAD_FILE_DWG="donut-master/download_data_for_training_dwg.py"
    python "$DOWNLOAD_FILE_DWG" "$1" "$2" "$3"
else
    echo "Invalid argument provided. Please use 'donut_invoice' or 'donut_dwg' as the 4th argument."
    exit 1
fi

DATASET_FILE="donut-master/run_donut.py"
python "$DATASET_FILE"

TRAINER_FILE="donut-master/donut_training.py"
python "$TRAINER_FILE" "$4"
