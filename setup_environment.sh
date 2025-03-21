#!/bin/bash
# Energy Monitoring Environment Setup Script

set -e  # Exit on any error

echo "Setting up Python environment for energy monitoring tools..."

# Create a virtual environment
python3 -m venv energy_monitor_env
source energy_monitor_env/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install required packages
pip install requests nvidia-ml-py3 pandas matplotlib

# Create directory structure
mkdir -p energy_monitor/{logs,scripts,results}