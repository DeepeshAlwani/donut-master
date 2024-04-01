#!/bin/bash

# Install Poppler (PDF rendering library) if not already installed
if ! command -v pdftoppm &> /dev/null; then
    echo "Installing Poppler..."
    sudo yum install -y poppler-utils
fi

token = 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

huggingface-cli login --token $token

# Path to the requirements.txt file
REQUIREMENTS_FILE="requirement.txt"

# Check if requirements.txt exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "requirements.txt not found"
    exit 1
fi



# Install libraries listed in requirements.txt
pip install -r "$REQUIREMENTS_FILE"
