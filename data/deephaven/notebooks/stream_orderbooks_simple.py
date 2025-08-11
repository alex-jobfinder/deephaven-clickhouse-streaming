#!/usr/bin/env python3
"""
Simplified DeepHaven script for streaming HyperLiquid orderbooks
Focuses on minimal processing and logging of raw data
"""

import deephaven.dtypes as dht
import deephaven.stream.kafka.consumer as ck
import json
from deephaven import merge
from deephaven.plot.figure import Figure
from deephaven.plot import PlotStyle, Color

# Configuration
KAFKA_BROKER = "redpanda:29092"
TOPIC = "orderbooks"

def log_orderbook_structure(orderbooks_table):
    """Log the structure of incoming orderbook data"""
    print("=" * 80)
    print("ORDERBOOK TABLE STRUCTURE:")
    print(f"Columns: {orderbooks_table.columns}")
    print(f"Row count: {orderbooks_table.size}")
    
    if orderbooks_table.size > 0:
        print("\nSAMPLE DATA:")
        sample = orderbooks_table.head(3).snapshot()
        for col in sample.columns:
            print(f"{col}: {sample.get_column(col).get(0)}")
    print("=" * 80)

def create_simple_orderbook_view(orderbooks_table):
    """Create a simple view with minimal processing"""
    return orderbooks_table.update([
        "ts_formatted = toString(ts)",
        "coin_upper = upper(coin)",
        "levels_count = length(levels_json)",
        "raw_data_length = length(raw_data)"
    ]).select([
        "ts",
        "ts_formatted", 
        "exchange",
        "symbol",
        "coin_upper",
        "time_ms",
        "levels_count",
        "raw_data_length",
        "levels_json",
        "raw_data"
    ])

def create_basic_plots(orderbooks_simple):
    """Create basic plots for visualization"""
    
    # Plot 1: Orderbook frequency over time
    plot_frequency = (
        Figure()
        .chart_title(title="HyperLiquid Orderbook Frequency (All Symbols)")
        .plot_xy(
            series_name="Orderbook Updates",
            t=orderbooks_simple,
            x="ts",
            y="levels_count"
        )
        .axes(plot_style=PlotStyle.SCATTER)
        .show()
    )
    
    # Plot 2: Timestamp analysis
    plot_timestamps = (
        Figure()
        .chart_title(title="Exchange vs Exchange Timestamp (All Symbols)")
        .plot_xy(
            series_name="Exchange Time",
            t=orderbooks_simple,
            x="ts",
            y="time_ms"
        )
        .axes(plot_style=PlotStyle.SCATTER)
        .show()
    )
    
    return plot_frequency, plot_timestamps

def analyze_levels_structure(orderbooks_simple):
    """Analyze the structure of the levels JSON"""
    print("\n" + "=" * 80)
    print("LEVELS JSON STRUCTURE ANALYSIS:")
    
    # Get a sample with actual data
    sample = orderbooks_simple.head(1).snapshot()
    
    if sample.size > 0:
        levels_json = sample.get_column("levels_json").get(0)
        raw_data = sample.get_column("raw_data").get(0)
        
        print(f"Raw levels JSON: {levels_json}")
        print(f"Raw data length: {len(raw_data)}")
        
        try:
            # Try to parse the levels JSON
            levels_parsed = json.loads(levels_json)
            print(f"Levels type: {type(levels_parsed)}")
            print(f"Levels length: {len(levels_parsed)}")
            
            if isinstance(levels_parsed, list) and len(levels_parsed) == 2:
                bids, asks = levels_parsed
                print(f"Bids count: {len(bids)}")
                print(f"Asks count: {len(asks)}")
                
                if bids:
                    print(f"Sample bid: {bids[0]}")
                if asks:
                    print(f"Sample ask: {asks[0]}")
                    
        except json.JSONDecodeError as e:
            print(f"Failed to parse levels JSON: {e}")
    
    print("=" * 80)

# Main execution
if __name__ == "__main__":
    print("Starting HyperLiquid Orderbook Stream (Simplified)")
    print(f"Connecting to Kafka: {KAFKA_BROKER}")
    print(f"Topic: {TOPIC}")
    
    # Create blink table from Kafka
    orderbooks_blink = ck.consume(
        {"bootstrap.servers": KAFKA_BROKER},
        TOPIC,
        key_spec=ck.KeyValueSpec.IGNORE,
        value_spec=ck.json_spec({
            "exchange": dht.string,
            "symbol": dht.string,
            "ts": dht.Instant,
            "raw_data": dht.string,  # Store complete raw message
            "coin": dht.string,      # Extract coin
            "time_ms": dht.uint64,   # Extract timestamp
            "levels_json": dht.string # Extract levels
        }),
        table_type=ck.TableType.blink(),
    )
    
    print(f"Connected to Kafka. Table created with {orderbooks_blink.size} initial rows")
    
    # Log the structure
    log_orderbook_structure(orderbooks_blink)
    
    # Create simple view
    orderbooks_simple = create_simple_orderbook_view(orderbooks_blink)
    
    print(f"Simple view created. Current rows: {orderbooks_simple.size}")
    
    # Analyze levels structure
    analyze_levels_structure(orderbooks_simple)
    
    # Create basic plots
    plot1, plot2 = create_basic_plots(orderbooks_simple)
    
    print("\n" + "=" * 80)
    print("SETUP COMPLETE!")
    print("Available tables:")
    print("- orderbooks_blink: Raw Kafka stream")
    print("- orderbooks_simple: Processed view")
    print("Available plots:")
    print("- plot_frequency: Orderbook update frequency")
    print("- plot_timestamps: Timestamp analysis")
    print("=" * 80)
    
    # Keep the script running and show live updates
    print("\nMonitoring orderbook stream... (Press Ctrl+C to stop)")
    
    # Show the simple table for live monitoring
    orderbooks_simple.show()
