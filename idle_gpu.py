#!/usr/bin/env python3
"""
High-Frequency GPU Energy Monitoring Script
-------------------------------------------
This script monitors GPU energy consumption using NVIDIA-SMI at high frequency.
Optimized for capturing rapid changes in power usage.

Requirements:
- Python 3.6+
- NVIDIA GPU with nvidia-smi utility
- pynvml package (pip install nvidia-ml-py3)
"""

import os
import time
import datetime
import argparse
import csv
import subprocess
from pathlib import Path

try:
    import pynvml
    PYNVML_AVAILABLE = True
except ImportError:
    PYNVML_AVAILABLE = False
    print("Warning: pynvml not installed. Falling back to nvidia-smi command line tool.")
    print("For better performance, install pynvml: pip install nvidia-ml-py3")

def check_nvidia_smi():
    """Check if nvidia-smi is available."""
    try:
        subprocess.run(["nvidia-smi", "-L"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        return False

def get_gpu_info_nvml():
    """Get GPU information using NVML library."""
    pynvml.nvmlInit()
    device_count = pynvml.nvmlDeviceGetCount()
    
    if device_count == 0:
        raise Exception("No NVIDIA GPUs found on the system.")
    
    gpus = []
    for i in range(device_count):
        handle = pynvml.nvmlDeviceGetHandleByIndex(i)
        # Handle string or bytes return value from nvmlDeviceGetName
        try:
            name_raw = pynvml.nvmlDeviceGetName(handle)
            # Check if it's bytes or string
            name = name_raw.decode('utf-8') if isinstance(name_raw, bytes) else name_raw
        except (AttributeError, UnicodeDecodeError):
            # Fallback if decoding fails
            name = str(name_raw)
            
        gpus.append({
            'index': i,
            'name': name,
            'handle': handle
        })
    
    return gpus

def get_gpu_power_nvml(handle):
    """Get GPU power consumption using NVML."""
    try:
        power = pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0  # convert from mW to W
        return power
    except pynvml.NVMLError:
        return None

def get_gpu_utilization_nvml(handle):
    """Get GPU utilization using NVML."""
    try:
        util = pynvml.nvmlDeviceGetUtilizationRates(handle)
        return {
            'gpu': util.gpu,
            'memory': util.memory
        }
    except pynvml.NVMLError:
        return {'gpu': None, 'memory': None}

def get_gpu_temperature_nvml(handle):
    """Get GPU temperature using NVML."""
    try:
        temp = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
        return temp
    except pynvml.NVMLError:
        return None

def get_gpu_memory_nvml(handle):
    """Get GPU memory info using NVML."""
    try:
        memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
        return {
            'total': memory.total / (1024 * 1024),  # MB
            'used': memory.used / (1024 * 1024),    # MB
            'free': memory.free / (1024 * 1024)     # MB
        }
    except pynvml.NVMLError:
        return {'total': None, 'used': None, 'free': None}

def get_clock_info_nvml(handle):
    """Get GPU clock information."""
    try:
        sm_clock = pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_SM)
        mem_clock = pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_MEM)
        return {
            'sm_clock': sm_clock,   # MHz
            'mem_clock': mem_clock  # MHz
        }
    except pynvml.NVMLError:
        return {'sm_clock': None, 'mem_clock': None}

def main():
    parser = argparse.ArgumentParser(description="High-Frequency GPU Energy Monitoring")
    parser.add_argument("-i", "--interval", type=float, default=0.01, 
                        help="Sampling interval in seconds (default: 0.01 = 10ms)")
    parser.add_argument("-d", "--duration", type=int, default=0,
                        help="Monitoring duration in seconds (default: 0, run until interrupted)")
    parser.add_argument("-o", "--output", type=str, default="gpu_energy_data.csv",
                        help="Output CSV file (default: gpu_energy_data.csv)")
    parser.add_argument("-b", "--buffer-size", type=int, default=1000,
                        help="Buffer size before writing to disk (default: 1000 samples)")
    parser.add_argument("--minimal", action="store_true",
                        help="Collect only power data for maximum sampling speed")
    args = parser.parse_args()
    
    # Check if nvidia-smi is available
    if not check_nvidia_smi():
        raise Exception("nvidia-smi not found. Please ensure NVIDIA drivers are installed correctly.")
    
    # For high frequency sampling, we must use NVML
    if not PYNVML_AVAILABLE:
        raise Exception("pynvml is required for high-frequency sampling. Install with: pip install nvidia-ml-py3")
    
    # Warn if interval is very small
    if args.interval < 0.005:
        print("Warning: Sampling interval is extremely small. This may impact system performance.")
        print("The actual interval may be limited by hardware and API capabilities.")
    
    try:
        # Initialize NVML and get GPU information
        gpus = get_gpu_info_nvml()
        print(f"Found {len(gpus)} NVIDIA GPUs:")
        for gpu in gpus:
            print(f"  - GPU {gpu['index']}: {gpu['name']}")
        
        # Create output file and write header
        with open(args.output, 'w', newline='') as csvfile:
            fieldnames = ['timestamp', 'elapsed_seconds']
            
            # Add fields for each GPU
            for gpu in gpus:
                prefix = f"gpu{gpu['index']}"
                fieldnames.append(f"{prefix}_power_watts")
                
                # Add additional fields unless in minimal mode
                if not args.minimal:
                    fieldnames.extend([
                        f"{prefix}_temperature_c",
                        f"{prefix}_utilization_gpu_percent",
                        f"{prefix}_utilization_memory_percent",
                        f"{prefix}_sm_clock_mhz",
                        f"{prefix}_mem_clock_mhz",
                        f"{prefix}_memory_used_mb"
                    ])
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Create buffer for samples to reduce disk I/O
            buffer = []
            
            start_time = time.time()
            print(f"Starting high-frequency GPU monitoring at {args.interval}s intervals...")
            print(f"Data will be saved to {args.output}")
            print("Press Ctrl+C to stop monitoring")
            
            try:
                samples = 0
                last_status_time = start_time
                
                while True:
                    sample_start = time.time()
                    
                    # Initialize row with timestamp
                    current_time = time.time()
                    elapsed_seconds = current_time - start_time
                    
                    # Check if duration limit reached
                    if args.duration > 0 and elapsed_seconds >= args.duration:
                        print(f"Reached specified duration of {args.duration} seconds.")
                        break
                    
                    row = {
                        'timestamp': datetime.datetime.now().isoformat(),
                        'elapsed_seconds': round(elapsed_seconds, 6)  # Microsecond precision
                    }
                    
                    # Get stats for each GPU
                    for gpu in gpus:
                        prefix = f"gpu{gpu['index']}"
                        
                        # Always get power consumption
                        power = get_gpu_power_nvml(gpu['handle'])
                        row[f"{prefix}_power_watts"] = round(power, 3) if power is not None else None
                        
                        # Get additional metrics unless in minimal mode
                        if not args.minimal:
                            temp = get_gpu_temperature_nvml(gpu['handle'])
                            util = get_gpu_utilization_nvml(gpu['handle'])
                            clocks = get_clock_info_nvml(gpu['handle'])
                            memory = get_gpu_memory_nvml(gpu['handle'])
                            
                            row[f"{prefix}_temperature_c"] = temp
                            row[f"{prefix}_utilization_gpu_percent"] = util['gpu']
                            row[f"{prefix}_utilization_memory_percent"] = util['memory']
                            row[f"{prefix}_sm_clock_mhz"] = clocks['sm_clock']
                            row[f"{prefix}_mem_clock_mhz"] = clocks['mem_clock']
                            row[f"{prefix}_memory_used_mb"] = round(memory['used'], 2) if memory['used'] is not None else None
                    
                    # Add to buffer
                    buffer.append(row)
                    samples += 1
                    
                    # Flush buffer when it reaches the buffer size
                    if len(buffer) >= args.buffer_size:
                        writer.writerows(buffer)
                        csvfile.flush()
                        buffer = []
                        
                        # Print status update
                        current = time.time()
                        if current - last_status_time >= 5:  # Status update every 5 seconds
                            rate = samples / (current - start_time)
                            print(f"[{row['timestamp']}] Collected {samples} samples ({rate:.2f} samples/sec)")
                            last_status_time = current
                    
                    # Calculate sleep time to maintain requested interval
                    sample_time = time.time() - sample_start
                    sleep_time = max(0, args.interval - sample_time)
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                        
            except KeyboardInterrupt:
                print("\nMonitoring stopped by user.")
            
            # Write any remaining buffer data
            if buffer:
                writer.writerows(buffer)
            
            # Calculate stats
            end_time = time.time()
            total_time = end_time - start_time
            avg_rate = samples / total_time if total_time > 0 else 0
            
            print(f"\nMonitoring complete.")
            print(f"Collected {samples} samples over {total_time:.2f} seconds")
            print(f"Average sampling rate: {avg_rate:.2f} samples/second")
            print(f"Data saved to {args.output}")
            
            # Clean up NVML
            pynvml.nvmlShutdown()
            
    except Exception as e:
        print(f"Error: {e}")
        if PYNVML_AVAILABLE:
            try:
                pynvml.nvmlShutdown()
            except:
                pass
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())