#!/bin/bash

curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.rpm.sh | sudo bash

sudo yum install git-lfs
git lfs install
git lfs version


# Path to the requirements.txt file
REQUIREMENTS_FILE="donut-master/requirment.txt"

# Check if requirements.txt exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "requirements.txt not found"
    exit 1
fi

pip install huggingface-cli

huggingface-cli login --token $3

git lfs install

git clone https://huggingface.co/DeepeshAlwani/donut-demo

# Install libraries listed in requirements.txt
pip install -r donut-master/requirment.txt

DOWNLOAD_FILE="donut-master/download_training_data.py"

python "$DOWNLOAD_FILE" $1 $2

DATASET_FILE="donut-master/run_donut.py"

python "$DATASET_FILE"

TRAINER_FILE="donut-master/donut_training.py"

python "$TRAINER_FILE"
