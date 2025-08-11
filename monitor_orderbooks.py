#!/usr/bin/env python3
"""
Orderbook Monitoring Dashboard
Quick script to check the health of orderbook data flow
"""

import asyncio
import time
from datetime import datetime, timedelta
import subprocess
import json

def run_clickhouse_query(query):
    """Execute a ClickHouse query and return results"""
    try:
        result = subprocess.run([
            'docker', 'exec', 'clickhouse', 'clickhouse-client', 
            '--query', query, '--format', 'JSONEachRow'
        ], capture_output=True, text=True, check=True)
        
        if result.stdout.strip():
            return [json.loads(line) for line in result.stdout.strip().split('\n')]
        return []
    except subprocess.CalledProcessError as e:
        print(f"ClickHouse query failed: {e}")
        return []

def check_kafka_topic():
    """Check Kafka topic status"""
    try:
        result = subprocess.run([
            'docker', 'exec', 'redpanda', 'rpk', 'topic', 'describe', 'orderbooks'
        ], capture_output=True, text=True, check=True)
        return "Topic exists" in result.stdout or "orderbooks" in result.stdout
    except:
        return False

def check_recent_data():
    """Check for recent orderbook data"""
    query = """
    SELECT 
        exchange,
        symbol,
        ts,
        length(bid) as bid_levels,
        length(ask) as ask_levels,
        now() - ts as seconds_ago
    FROM cryptofeed.orderbooks 
    WHERE ts > now() - INTERVAL 5 MINUTE
    ORDER BY ts DESC 
    LIMIT 5
    """
    return run_clickhouse_query(query)

def check_data_rates():
    """Check data ingestion rates"""
    query = """
    SELECT 
        symbol,
        COUNT(*) as message_count,
        MIN(ts) as first_message,
        MAX(ts) as last_message
    FROM cryptofeed.orderbooks 
    WHERE ts > now() - INTERVAL 1 HOUR
    GROUP BY symbol
    ORDER BY message_count DESC
    """
    return run_clickhouse_query(query)

def main():
    print("=" * 80)
    print("📊 ORDERBOOK MONITORING DASHBOARD")
    print("=" * 80)
    print(f"Timestamp: {datetime.now()}")
    print()
    
    # 1. Check Kafka topic
    print("🔍 KAFKA TOPIC STATUS:")
    kafka_ok = check_kafka_topic()
    print(f"   Orderbooks topic: {'✅ Active' if kafka_ok else '❌ Not found'}")
    print()
    
    # 2. Check recent data
    print("📈 RECENT ORDERBOOK DATA (last 5 minutes):")
    recent_data = check_recent_data()
    if recent_data:
        for row in recent_data:
            print(f"   {row['symbol']}: {row['bid_levels']} bids, {row['ask_levels']} asks "
                  f"({row['seconds_ago']:.1f}s ago)")
    else:
        print("   ❌ No recent data found")
    print()
    
    # 3. Check data rates
    print("📊 DATA INGESTION RATES (last hour):")
    rates = check_data_rates()
    if rates:
        for row in rates:
            rate = row['message_count'] / 60  # per minute
            print(f"   {row['symbol']}: {row['message_count']} messages ({rate:.1f}/min)")
    else:
        print("   ❌ No data in last hour")
    print()
    
    # 4. Check for issues
    print("🔍 HEALTH CHECK:")
    issues = []
    
    if not kafka_ok:
        issues.append("Kafka topic not accessible")
    
    if not recent_data:
        issues.append("No recent orderbook data")
    elif len(recent_data) < 2:
        issues.append("Very low data volume")
    
    if not rates:
        issues.append("No data ingestion in last hour")
    
    if issues:
        print("   ❌ Issues found:")
        for issue in issues:
            print(f"      - {issue}")
    else:
        print("   ✅ System appears healthy")
    
    print("=" * 80)

if __name__ == "__main__":
    main()
