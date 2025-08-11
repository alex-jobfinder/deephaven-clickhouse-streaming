#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p logs/combined

# Function to monitor logs in real-time
monitor_logs() {
    echo "🔍 Monitoring all container logs..."
    echo "Press Ctrl+C to stop monitoring"
    echo "----------------------------------------"
    
    # Monitor all containers simultaneously
    docker-compose logs -f --tail=100 | tee logs/combined/$(date +%Y%m%d_%H%M%S)_all.log
}

# Function to check for errors
check_errors() {
    echo "🚨 Checking for errors in recent logs..."
    
    # Check cryptofeed errors
    if docker logs cryptofeed 2>&1 | grep -i "error\|exception\|failed" > /dev/null; then
        echo "❌ Errors found in cryptofeed:"
        docker logs cryptofeed 2>&1 | grep -i "error\|exception\|failed" | tail -5
    else
        echo "✅ No errors in cryptofeed"
    fi
    
    # Check deephaven errors
    if docker logs deephaven 2>&1 | grep -i "error\|exception\|failed" > /dev/null; then
        echo "❌ Errors found in deephaven:"
        docker logs deephaven 2>&1 | grep -i "error\|exception\|failed" | tail -5
    else
        echo "✅ No errors in deephaven"
    fi
    
    # Check clickhouse errors
    if docker logs clickhouse 2>&1 | grep -i "error\|exception\|failed" > /dev/null; then
        echo "❌ Errors found in clickhouse:"
        docker logs clickhouse 2>&1 | grep -i "error\|exception\|failed" | tail -5
    else
        echo "✅ No errors in clickhouse"
    fi
}

# Function to show log file sizes
show_log_sizes() {
    echo "📊 Log file sizes:"
    du -h logs/*/*.log 2>/dev/null | sort -hr | head -10
}

# Main menu
case "${1:-monitor}" in
    "monitor")
        monitor_logs
        ;;
    "errors")
        check_errors
        ;;
    "sizes")
        show_log_sizes
        ;;
    "help")
        echo "Usage: $0 [monitor|errors|sizes|help]"
        echo "  monitor - Monitor all logs in real-time (default)"
        echo "  errors  - Check for recent errors"
        echo "  sizes   - Show log file sizes"
        echo "  help    - Show this help"
        ;;
    *)
        echo "Unknown option: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac
