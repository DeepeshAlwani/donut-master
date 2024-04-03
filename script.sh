#!/bin/bash

sudo yum install -y amazon-linux-extras
sudo amazon-linux-extras install epel -y
sudo yum-config-manager --enable epel
sudo yum install -y git-lfs
git lfs install
git lfs version


# Path to the requirements.txt file
REQUIREMENTS_FILE="donut-master/requirment.txt"

# Check if requirements.txt exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "requirements.txt not found"
    exit 1
fi

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
