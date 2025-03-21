#!/usr/bin/env python3
"""
High-Frequency CPU Energy Monitoring Script using Intel RAPL
-----------------------------------------------------------
This script monitors CPU energy consumption using Intel's Running Average Power Limit (RAPL)
interface at high frequency. Optimized for capturing rapid changes in power usage.

Requirements:
- Python 3.6+
- Root privileges (or appropriate permissions to access RAPL sysfs files)
- Linux system with Intel CPU supporting RAPL
"""

import os
import time
import datetime
import argparse
import csv
from pathlib import Path
import threading

# RAPL sysfs path
RAPL_PATH = "/sys/class/powercap/intel-rapl"

class RaplReader:
    """Fast reader for RAPL energy values."""
    
    def __init__(self, domain_paths):
        """Initialize with paths to monitor."""
        self.domain_paths = domain_paths
        self.energy_paths = {domain: os.path.join(path, "energy_uj") 
                             for domain, path in domain_paths.items()}
        self.max_energy_paths = {domain: os.path.join(path, "max_energy_range_uj") 
                                for domain, path in domain_paths.items()}
        
        # Cache max energy values
        self.max_energy_values = {}
        for domain, path in self.max_energy_paths.items():
            try:
                with open(path, 'r') as f:
                    self.max_energy_values[domain] = int(f.read().strip())
            except (IOError, OSError) as e:
                print(f"Warning: Couldn't read max energy for {domain}: {e}")
                self.max_energy_values[domain] = 2**32  # Fallback value
    
    def read_energy_values(self):
        """Read energy values for all domains in parallel."""
        result = {}
        
        def read_domain(domain):
            try:
                with open(self.energy_paths[domain], 'r') as f:
                    result[domain] = int(f.read().strip())
            except (IOError, OSError) as e:
                result[domain] = None
        
        # Use threads for faster parallel reads
        threads = []
        for domain in self.domain_paths:
            thread = threading.Thread(target=read_domain, args=(domain,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to finish
        for thread in threads:
            thread.join()
            
        return result
    
    def get_max_energy(self, domain):
        """Get max energy value for a domain."""
        return self.max_energy_values.get(domain)

def get_available_domains():
    """Get all available RAPL domains on the system."""
    domains = {}
    
    if not os.path.exists(RAPL_PATH):
        raise Exception("RAPL sysfs interface not found. Ensure your CPU supports RAPL and it's enabled.")
    
    # Find all intel-rapl domains
    for domain in os.listdir(RAPL_PATH):
        if domain.startswith("intel-rapl:"):
            domain_path = os.path.join(RAPL_PATH, domain)
            
            # Get domain name
            with open(os.path.join(domain_path, "name"), 'r') as f:
                name = f.read().strip()
            
            domains[name] = domain_path
            
            # Check for subdomains
            for subdomain in os.listdir(domain_path):
                if subdomain.startswith("intel-rapl:"):
                    subdomain_path = os.path.join(domain_path, subdomain)
                    
                    # Get subdomain name
                    with open(os.path.join(subdomain_path, "name"), 'r') as f:
                        subname = f.read().strip()
                    
                    domains[f"{name}-{subname}"] = subdomain_path
    
    return domains

def main():
    parser = argparse.ArgumentParser(description="High-Frequency CPU Energy Monitoring using Intel RAPL")
    parser.add_argument("-i", "--interval", type=float, default=0.01, 
                        help="Sampling interval in seconds (default: 0.01 = 10ms)")
    parser.add_argument("-d", "--duration", type=int, default=0,
                        help="Monitoring duration in seconds (default: 0, run until interrupted)")
    parser.add_argument("-o", "--output", type=str, default="cpu_energy_data.csv",
                        help="Output CSV file (default: cpu_energy_data.csv)")
    parser.add_argument("-b", "--buffer-size", type=int, default=1000,
                        help="Buffer size before writing to disk (default: 1000 samples)")
    parser.add_argument("--domains", type=str,
                        help="Comma-separated list of specific RAPL domains to monitor (default: all)")
    args = parser.parse_args()
    
    # Warn if interval is very small
    if args.interval < 0.005:
        print("Warning: Sampling interval is extremely small. This may impact system performance.")
        print("The actual interval may be limited by hardware and kernel capabilities.")
    
    try:
        # Get available domains
        all_domains = get_available_domains()
        
        if not all_domains:
            raise Exception("No RAPL domains found on this system.")
        
        # Filter domains if specified
        if args.domains:
            requested_domains = args.domains.split(',')
            domains = {k: v for k, v in all_domains.items() if k in requested_domains}
            if not domains:
                print(f"Warning: None of the requested domains {requested_domains} were found.")
                print(f"Available domains: {list(all_domains.keys())}")
                domains = all_domains
        else:
            domains = all_domains
        
        print(f"Found {len(domains)} RAPL domains:")
        for domain, path in domains.items():
            print(f"  - {domain} ({path})")
        
        # Initialize RAPL reader
        rapl_reader = RaplReader(domains)
        
        # Initialize energy readings
        prev_energy = rapl_reader.read_energy_values()
        
        # Create output file and write header
        with open(args.output, 'w', newline='') as csvfile:
            fieldnames = ['timestamp', 'elapsed_seconds'] + [f"{domain}_power_watts" for domain in domains]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Create buffer for samples to reduce disk I/O
            buffer = []
            
            start_time = time.time()
            print(f"Starting high-frequency CPU energy monitoring at {args.interval}s intervals...")
            print(f"Data will be saved to {args.output}")
            print("Press Ctrl+C to stop monitoring")
            
            try:
                samples = 0
                last_status_time = start_time
                
                while True:
                    sample_start = time.time()
                    
                    # Check if duration limit reached
                    current_time = time.time()
                    elapsed_seconds = current_time - start_time
                    
                    if args.duration > 0 and elapsed_seconds >= args.duration:
                        print(f"Reached specified duration of {args.duration} seconds.")
                        break
                    
                    # Read current energy values
                    current_energy = rapl_reader.read_energy_values()
                    
                    # Calculate power for each domain
                    row = {
                        'timestamp': datetime.datetime.now().isoformat(),
                        'elapsed_seconds': round(elapsed_seconds, 6)  # Microsecond precision
                    }
                    
                    for domain in domains:
                        if prev_energy[domain] is not None and current_energy[domain] is not None:
                            # Handle energy counter wraparound
                            energy_diff = current_energy[domain] - prev_energy[domain]
                            if energy_diff < 0:
                                max_range = rapl_reader.get_max_energy(domain)
                                energy_diff += max_range
                            
                            # Calculate power in watts (energy in joules / time in seconds)
                            # Use actual elapsed time for better accuracy with small intervals
                            actual_interval = current_time - sample_start + args.interval
                            power_watts = (energy_diff / 1000000) / actual_interval
                            row[f"{domain}_power_watts"] = round(power_watts, 3)
                        else:
                            row[f"{domain}_power_watts"] = None
                    
                    # Update previous energy values
                    prev_energy = current_energy
                    
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
                            total_power = sum(v for k, v in row.items() if k.endswith('_power_watts') and v is not None)
                            print(f"[{row['timestamp']}] Collected {samples} samples ({rate:.2f} samples/sec), "
                                  f"Total CPU Power: {round(total_power, 2)}W")
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
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())