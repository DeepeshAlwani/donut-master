#!/bin/bash

# Install Poppler (PDF rendering library) if not already installed
if ! command -v pdftoppm &> /dev/null; then
    echo "Installing Poppler..."
    sudo yum install -y poppler-utils
fi


# Path to the requirements.txt file
REQUIREMENTS_FILE="donut-master/requirment.txt"

# Check if requirements.txt exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "requirements.txt not found"
    exit 1
fi

sudo yum install python3

python3 -m venv myenv
source myenv/bin/activate

pip install huggingface-cli

huggingface-cli login --token $3

# Install libraries listed in requirements.txt
pip install -r donut-master/requirment.txt

DOWNLOAD_FILE="donut-master/download_training_data.py"

python "$DOWNLOAD_FILE" $1 $2

DATASET_FILE="donut-master/run_donut.py"

python "$DATASET_FILE"

TRAINER_FILE="donut-master/donut_training.py"

python "$TRAINER_FILE"
