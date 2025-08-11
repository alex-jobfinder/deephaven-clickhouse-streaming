#!/bin/bash
# Quick test script to run inside Docker container

echo "Testing cryptofeed orderbook script inside Docker container..."

# Navigate to the source directory
cd /app/src

# Run the orderbook script
python script/cryptofeed_2_orderbooks.py

echo "Test completed!"
