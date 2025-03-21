#!/bin/bash

# Set output filename
OUTPUT_FILE="power_readings_$(date +%Y%m%d_%H%M%S).csv"

# Create CSV header
echo "Timestamp,UnixTimestamp,PowerReading_Watts" > $OUTPUT_FILE

# Set sampling duration (seconds)
DURATION=3600  # Default 1 hour, adjust as needed

# Calculate end time
END_TIME=$(($(date +%s) + DURATION))

echo "Starting power data collection, once per second, for ${DURATION} seconds..."
echo "Results will be saved to: $OUTPUT_FILE"

# Run sampling loop
while [ $(date +%s) -lt $END_TIME ]; do
    # Get current timestamp
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    UNIX_TIME=$(date +%s)
    
    # Get power reading (extract instantaneous power value only)
    POWER=$(ipmitool dcmi power reading | grep "Instantaneous power reading" | awk '{print $4}')
    
    # Write to CSV
    echo "$TIMESTAMP,$UNIX_TIME,$POWER" >> $OUTPUT_FILE
    
    # Wait until the start of the next second
    sleep 1
done

echo "Sampling complete. Collected $(( $(wc -l < $OUTPUT_FILE) - 1 )) data points."