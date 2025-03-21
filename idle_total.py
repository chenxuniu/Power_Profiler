#!/usr/bin/env python3
"""
High-Frequency Total System Energy Monitoring Script using iDRAC/Redfish API
---------------------------------------------------------------------------
This script monitors server power consumption using the Dell iDRAC Redfish API
at high frequency. Optimized for maximum sampling rate.

Requirements:
- Python 3.6+
- requests package (pip install requests)
- Dell server with iDRAC configured for Redfish API access
"""

import os
import sys
import time
import datetime
import argparse
import csv
import json
import urllib3
import requests
from requests.auth import HTTPBasicAuth
from pathlib import Path
import threading
from collections import deque
import statistics

# Disable insecure HTTPS warnings (optional, for self-signed certificates)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class RedfishClient:
    """Client for interacting with Redfish API on Dell iDRAC."""
    
    def __init__(self, host, username, password, verify_ssl=False, timeout=5):
        self.base_url = f"https://{host}/redfish/v1"
        self.auth = HTTPBasicAuth(username, password)
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.session = requests.Session()
        
        # Cache for power URIs to reduce API calls
        self._power_uri_cache = {}
        
        # Connection optimization - set persistent connection
        self.session.headers.update({
            'Connection': 'keep-alive',
            'Accept': 'application/json'
        })
    
    def _request(self, method, path, **kwargs):
        """Make a request to the Redfish API."""
        url = f"{self.base_url}{path}"
        
        kwargs.setdefault('auth', self.auth)
        kwargs.setdefault('verify', self.verify_ssl)
        kwargs.setdefault('timeout', self.timeout)
        
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json() if response.content else None
        except requests.exceptions.RequestException as e:
            print(f"Error in Redfish API request: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status code: {e.response.status_code}")
                try:
                    error_data = e.response.json()
                    print(f"Error details: {json.dumps(error_data, indent=2)}")
                except:
                    print(f"Response text: {e.response.text}")
            raise
    
    def get_power_uri(self, system_id="/Systems/System.Embedded.1"):
        """Get the power URI for a system (with caching)."""
        if system_id in self._power_uri_cache:
            return self._power_uri_cache[system_id]
        
        system = self._request('GET', system_id)
        power_uri = system.get('Power', {}).get('@odata.id')
        
        if not power_uri:
            raise Exception(f"Power URI not found for system {system_id}")
        
        # Remove the base URL if present
        power_uri = power_uri.replace(self.base_url, '')
        
        # Cache the result
        self._power_uri_cache[system_id] = power_uri
        return power_uri
    
    def get_power_consumption(self, system_id="/Systems/System.Embedded.1"):
        """Get current power consumption in watts."""
        power_uri = self.get_power_uri(system_id)
        power_data = self._request('GET', power_uri)
        
        # Dell iDRAC specific format
        if 'PowerControl' in power_data:
            for control in power_data['PowerControl']:
                if 'PowerConsumedWatts' in control:
                    return control['PowerConsumedWatts']
        
        # Check PowerMetrics for some Redfish implementations
        for control in power_data.get('PowerControl', []):
            metrics = control.get('PowerMetrics', {})
            if 'AverageConsumedWatts' in metrics:
                return metrics['AverageConsumedWatts']
        
        raise Exception("Power consumption data not found in Redfish response")
    
    def get_power_supplies(self, system_id="/Systems/System.Embedded.1"):
        """Get power supply information."""
        power_uri = self.get_power_uri(system_id)
        power_data = self._request('GET', power_uri)
        
        power_supplies = []
        for supply in power_data.get('PowerSupplies', []):
            supply_info = {
                'id': supply.get('MemberId') or supply.get('Id'),
                'input_watts': supply.get('PowerInputWatts'),
                'output_watts': supply.get('PowerOutputWatts'),
                'state': supply.get('Status', {}).get('State')
            }
            power_supplies.append(supply_info)
        
        return power_supplies
    
    def close(self):
        """Close the session."""
        self.session.close()

class AsyncPowerMonitor:
    """Asynchronous power monitor that continuously polls in the background."""
    
    def __init__(self, client, system_id, max_samples=1000):
        self.client = client
        self.system_id = system_id
        self.samples = deque(maxlen=max_samples)
        self.running = False
        self.thread = None
        self.lock = threading.Lock()
        self.last_error = None
        self.poll_interval = 0.05  # 50ms between polls
    
    def start(self):
        """Start the monitoring thread."""
        if self.running:
            return False
        
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop)
        self.thread.daemon = True
        self.thread.start()
        return True
    
    def stop(self):
        """Stop the monitoring thread."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
            self.thread = None
    
    def _monitor_loop(self):
        """Background monitoring loop."""
        while self.running:
            try:
                # Get power consumption
                power = self.client.get_power_consumption(self.system_id)
                
                # Get power supplies (less frequently)
                power_supplies = []
                if len(self.samples) % 20 == 0:  # Every 20th sample
                    power_supplies = self.client.get_power_supplies(self.system_id)
                
                # Record sample with timestamp
                sample = {
                    'timestamp': datetime.datetime.now().isoformat(),
                    'time': time.time(),
                    'power': power,
                    'power_supplies': power_supplies
                }
                
                with self.lock:
                    self.samples.append(sample)
                
                # Sleep a bit to avoid overwhelming the iDRAC
                time.sleep(self.poll_interval)
                
            except Exception as e:
                self.last_error = str(e)
                # Sleep longer on error
                time.sleep(1.0)
    
    def get_samples(self, since_time=None):
        """Get collected samples, optionally filtered by time."""
        with self.lock:
            if since_time is not None:
                return [s for s in self.samples if s['time'] >= since_time]
            else:
                return list(self.samples)
    
    def get_latest_sample(self):
        """Get the most recent sample."""
        with self.lock:
            if self.samples:
                return self.samples[-1]
            return None
    
    def get_sampling_rate(self):
        """Estimate the current sampling rate in samples per second."""
        with self.lock:
            if len(self.samples) < 2:
                return 0
            
            # Get timestamps from last 10 samples or all samples if fewer
            count = min(10, len(self.samples))
            recent_samples = list(self.samples)[-count:]
            
            if count < 2:
                return 0
            
            # Calculate time difference between first and last sample
            time_diff = recent_samples[-1]['time'] - recent_samples[0]['time']
            if time_diff <= 0:
                return 0
            
            # Return rate (samples / time)
            return (count - 1) / time_diff

def main():
    parser = argparse.ArgumentParser(description="High-Frequency System Power Monitoring with iDRAC")
    parser.add_argument("--host", type=str, required=True,
                        help="iDRAC hostname or IP address")
    parser.add_argument("--username", type=str, required=True,
                        help="iDRAC username")
    parser.add_argument("--password", type=str, required=True,
                        help="iDRAC password")
    parser.add_argument("--system-id", type=str, default="/Systems/System.Embedded.1",
                        help="System ID (default: /Systems/System.Embedded.1)")
    parser.add_argument("-i", "--interval", type=float, default=0.1, 
                        help="Sampling interval in seconds (default: 0.1)")
    parser.add_argument("-d", "--duration", type=int, default=0,
                        help="Monitoring duration in seconds (default: 0, run until interrupted)")
    parser.add_argument("-o", "--output", type=str, default="system_power_data.csv",
                        help="Output CSV file (default: system_power_data.csv)")
    parser.add_argument("-b", "--buffer-size", type=int, default=1000,
                        help="Buffer size before writing to disk (default: 1000 samples)")
    parser.add_argument("--verify-ssl", action="store_true",
                        help="Verify SSL certificates (default: False)")
    parser.add_argument("--test", action="store_true",
                        help="Test connection and exit")
    args = parser.parse_args()
    
    try:
        # Create Redfish client
        client = RedfishClient(
            host=args.host,
            username=args.username,
            password=args.password,
            verify_ssl=args.verify_ssl,
            timeout=5
        )
        
        # Test connection if requested
        if args.test:
            try:
                power = client.get_power_consumption(args.system_id)
                power_supplies = client.get_power_supplies(args.system_id)
                
                print("Successfully connected to iDRAC Redfish API!")
                print(f"Current power consumption: {power} Watts")
                print(f"Found {len(power_supplies)} power supplies")
                
                # Print power supply information
                for ps in power_supplies:
                    print(f"  - Power Supply {ps['id']}: Output: {ps['output_watts']} W, State: {ps['state']}")
                
                client.close()
                return 0
            except Exception as e:
                print(f"Connection test failed: {e}")
                client.close()
                return 1
        
        # Create async power monitor
        monitor = AsyncPowerMonitor(client, args.system_id)
        monitor.start()
        
        print("Starting asynchronous power monitoring...")
        print("Waiting 2 seconds to collect initial samples...")
        time.sleep(2)  # Give monitor time to collect some initial samples
        
        # Check if we're getting samples
        initial_samples = monitor.get_samples()
        if not initial_samples:
            raise Exception("No power samples collected. Check iDRAC connection.")
        
        print(f"Successfully collecting power data. Current rate: {monitor.get_sampling_rate():.2f} samples/second")
        
        # Get power supply IDs for CSV header
        latest_sample = monitor.get_latest_sample()
        power_supply_ids = []
        if latest_sample and latest_sample['power_supplies']:
            power_supply_ids = [ps['id'] for ps in latest_sample['power_supplies']]
        
        # Create output file and write header
        with open(args.output, 'w', newline='') as csvfile:
            fieldnames = ['timestamp', 'elapsed_seconds', 'total_power_watts']
            
            # Add fields for power supplies
            for ps_id in power_supply_ids:
                fieldnames.append(f"ps_{ps_id}_output_watts")
                fieldnames.append(f"ps_{ps_id}_input_watts")
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Create buffer for samples to reduce disk I/O
            buffer = []
            
            start_time = time.time()
            print(f"Starting high-frequency system power monitoring...")
            print(f"Data will be saved to {args.output}")
            print("Press Ctrl+C to stop monitoring")
            
            next_sample_time = start_time
            last_status_time = start_time
            samples_collected = 0
            
            try:
                while True:
                    current_time = time.time()
                    
                    # Check if duration limit reached
                    if args.duration > 0 and (current_time - start_time) >= args.duration:
                        print(f"Reached specified duration of {args.duration} seconds.")
                        break
                    
                    # Check if it's time for next sample
                    if current_time >= next_sample_time:
                        # Get all new samples since last check
                        elapsed_seconds = current_time - start_time
                        latest_sample = monitor.get_latest_sample()
                        
                        if latest_sample:
                            # Create row with timestamp and power data
                            row = {
                                'timestamp': latest_sample['timestamp'],
                                'elapsed_seconds': round(elapsed_seconds, 6),
                                'total_power_watts': latest_sample['power']
                            }
                            
                            # Add power supply data if available
                            if latest_sample['power_supplies']:
                                for ps in latest_sample['power_supplies']:
                                    ps_id = ps['id']
                                    row[f"ps_{ps_id}_output_watts"] = ps['output_watts']
                                    row[f"ps_{ps_id}_input_watts"] = ps['input_watts']
                            
                            # Add to buffer
                            buffer.append(row)
                            samples_collected += 1
                        
                        # Calculate next sample time
                        next_sample_time = current_time + args.interval
                    
                    # Flush buffer when it reaches the buffer size
                    if len(buffer) >= args.buffer_size:
                        writer.writerows(buffer)
                        csvfile.flush()
                        buffer = []
                        
                        # Print status update
                        current = time.time()
                        if current - last_status_time >= 5:  # Status update every 5 seconds
                            actual_rate = samples_collected / (current - start_time)
                            backend_rate = monitor.get_sampling_rate()
                            
                            print(f"[{datetime.datetime.now().isoformat()}] "
                                  f"Collected {samples_collected} samples ({actual_rate:.2f} samples/sec), "
                                  f"Backend rate: {backend_rate:.2f} samples/sec, "
                                  f"Latest power: {latest_sample['power']}W")
                            
                            last_status_time = current
                    
                    # Sleep a bit to reduce CPU usage
                    time.sleep(min(0.01, max(0, next_sample_time - time.time())))
                        
            except KeyboardInterrupt:
                print("\nMonitoring stopped by user.")
            
            # Write any remaining buffer data
            if buffer:
                writer.writerows(buffer)
            
            # Calculate stats
            end_time = time.time()
            total_time = end_time - start_time
            avg_rate = samples_collected / total_time if total_time > 0 else 0
            
            print(f"\nMonitoring complete.")
            print(f"Collected {samples_collected} samples over {total_time:.2f} seconds")
            print(f"Average sampling rate: {avg_rate:.2f} samples/second")
            print(f"Data saved to {args.output}")
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        # Clean up
        if 'monitor' in locals():
            monitor.stop()
        if 'client' in locals():
            client.close()
    
    return 0

if __name__ == "__main__":
    exit(main())