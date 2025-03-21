# High-Frequency Energy Monitoring for GPU Nodes

A comprehensive toolkit for high-precision energy monitoring of GPU compute nodes. IdleWatts provides microsecond-level precision to capture both idle and active power states, supporting energy efficiency research, optimization, and benchmarking in high-performance computing environments.

## 🚀 Features

- **High-frequency sampling** (up to 100Hz) of CPU power using Intel RAPL
- **Detailed GPU power monitoring** via NVIDIA SMI with support for multiple GPUs
- **System-level power monitoring** through iDRAC/Redfish API
- **Combined monitoring** for synchronized data collection across components
- **Customizable sampling** intervals and duration
- **Data export** to CSV for further analysis
- **Visualization tools** for power consumption patterns
- **Support for both idle and load** power profiling

## 📋 Components

The toolkit includes multiple modules:

### Idle Power Profiling
- `idle_cpu_high_freq.py`: CPU power monitoring using Intel RAPL
- `idle_gpu_high_freq.py`: GPU power monitoring using NVIDIA-SMI
- `idle_system_high_freq.py`: System power monitoring using iDRAC/Redfish
- `run_energy_monitoring.sh`: Combined monitoring script

### Load Power Profiling
- Tools for measuring energy consumption under various workloads
- Benchmark suites for standardized comparisons
- Application-specific power profiling

### Analysis Tools
- Scripts for data processing and visualization
- Statistical analysis of power patterns
- Component-level energy breakdown

## 🔧 Requirements

- Python 3.6+
- For CPU monitoring: 
  - Intel CPU with RAPL support
  - Root access
- For GPU monitoring:
  - NVIDIA GPU
  - NVIDIA drivers with nvidia-smi
  - `nvidia-ml-py3` Python package
- For system monitoring:
  - Dell server with iDRAC
  - Network access to iDRAC interface
  - `requests` Python package

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/idlewatts.git
cd idlewatts

# Set up virtual environment
python -m venv energy_monitor_env
source energy_monitor_env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Make scripts executable
chmod +x *.py *.sh
```

## 💻 Usage

### Idle Power Monitoring

```bash
# Run the combined monitoring script
./run_energy_monitoring.sh [DURATION_SECONDS] [INTERVAL_SECONDS]

# Examples:
# Monitor for 1 hour with 10ms sampling
./run_energy_monitoring.sh 3600 0.01

# Monitor indefinitely until manually stopped
./run_energy_monitoring.sh 0 0.01
```

### Individual Component Monitoring

```bash
# CPU monitoring only
sudo python idle_cpu_high_freq.py -i 0.01 -o cpu_data.csv

# GPU monitoring only
python idle_gpu_high_freq.py -i 0.01 -o gpu_data.csv

# System monitoring (if iDRAC is accessible)
python idle_system_high_freq.py --host IDRAC_IP --username USER --password PASS -i 0.1
```

### Analyzing Results

```bash
# Analyze collected data
python analyze_energy_data.py --cpu-data cpu_data.csv --gpu-data gpu_data.csv

# Generate visualizations
python visualize_power.py --data-dir ./energy_data_YYYYMMDD_HHMMSS/
```

## 🔍 Use Cases

- Baseline energy benchmarking for GPU nodes
- Identification of power optimization opportunities
- Energy efficiency research in HPC environments
- Supporting green computing initiatives
- Validating power management policies
- Component-level power attribution

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgements

- Intel for RAPL technology
- NVIDIA for power monitoring capabilities
- Dell for iDRAC API documentation
