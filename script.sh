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

arg4=$4

# Conditional statements to check the value of the 4th argument
if [ "$arg4" == "donut-invoice" ]; then
  # Run specific scripts for 'donut-invoice'
    DOWNLOAD_FILE="donut-master/download_training_data.py"
    python "$DOWNLOAD_FILE" $1 $2 $3
elif [ "$arg4" == "donut-dwg" ]; then
  # Run specific scripts for 'donut-dwg'
  python3 script3.py
  python3 script4.py
else
  echo "Invalid argument provided. Please use 'donut-invoice' or 'donut-dwg' as the 4th argument."
fi

DATASET_FILE="donut-master/run_donut.py"

python "$DATASET_FILE"

TRAINER_FILE="donut-master/donut_training.py"

python "$TRAINER_FILE" $4
