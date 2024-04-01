#!/bin/bash

# Install Poppler (PDF rendering library) if not already installed
if ! command -v pdftoppm &> /dev/null; then
    echo "Installing Poppler..."
    sudo yum install -y poppler-utils
fi


huggingface-cli login --token $3

# Path to the requirements.txt file
REQUIREMENTS_FILE="requirement.txt"

# Check if requirements.txt exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "requirements.txt not found"
    exit 1
fi



# Install libraries listed in requirements.txt
pip install -r "$REQUIREMENTS_FILE"

DOWNLOAD_FILE="download_training_data.py"

python "$DOWNLOAD_FILE" $1 $2

DATASET_FILE="run_donut.py"

python "$DATASET_FILE"

TRAINER_FILE="donut_training.py"

python "$TRAINER_FILE"