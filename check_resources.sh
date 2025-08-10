#!/bin/bash

echo "🐳 Docker Compose Resource Check"
echo "================================"

echo ""
echo "💾 MEMORY:"
echo "Total RAM: $(free -h | grep '^Mem:' | awk '{print $2}')"
echo "Available RAM: $(free -h | grep '^Mem:' | awk '{print $7}')"
echo "Used RAM: $(free -h | grep '^Mem:' | awk '{print $3}')"
echo "Free RAM: $(free -h | grep '^Mem:' | awk '{print $4}')"

echo ""
echo "🖥️  CPU:"
echo "CPU Cores: $(nproc)"
echo "CPU Model: $(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
echo "Load Average: $(uptime | awk -F'load average:' '{print $2}')"

echo ""
echo "💿 DISK:"
echo "Disk Usage:"
df -h | grep -E '^/dev/|^Filesystem' | head -10

echo ""
echo "🐳 DOCKER:"
echo "Docker Version: $(docker --version 2>/dev/null || echo 'Docker not installed')"
echo "Docker Compose Version: $(docker-compose --version 2>/dev/null || echo 'Docker Compose not installed')"
echo "Running Containers: $(docker ps -q | wc -l)"

echo ""
echo "�� RECOMMENDATIONS:"
echo "================================"

# Get available RAM in GB
available_ram_gb=$(free -g | grep '^Mem:' | awk '{print $7}')

if [ "$available_ram_gb" -ge 32 ]; then
    echo "✅ RAM: Excellent! You have ${available_ram_gb}GB available"
    echo "   → Can allocate: DeepHaven: 24GB, ClickHouse: 8GB, RedPanda: 4GB"
elif [ "$available_ram_gb" -ge 24 ]; then
    echo "⚠️  RAM: Good! You have ${available_ram_gb}GB available"
    echo "   → Can allocate: DeepHaven: 16GB, ClickHouse: 6GB, RedPanda: 2GB"
elif [ "$available_ram_gb" -ge 16 ]; then
    echo "⚠️  RAM: Limited! You have ${available_ram_gb}GB available"
    echo "   → Can allocate: DeepHaven: 12GB, ClickHouse: 3GB, RedPanda: 1GB"
else
    echo "❌ RAM: Insufficient! Only ${available_ram_gb}GB available"
    echo "   → Consider upgrading RAM or reducing allocations"
fi

# Get CPU cores
cpu_cores=$(nproc)
if [ "$cpu_cores" -ge 8 ]; then
    echo "✅ CPU: Excellent! You have ${cpu_cores} cores"
    echo "   → Can allocate: DeepHaven: 4, ClickHouse: 2, RedPanda: 2"
elif [ "$cpu_cores" -ge 4 ]; then
    echo "⚠️  CPU: Good! You have ${cpu_cores} cores"
    echo "   → Can allocate: DeepHaven: 2, ClickHouse: 1, RedPanda: 1"
else
    echo "❌ CPU: Limited! Only ${cpu_cores} cores"
    echo "   → Consider reducing concurrent workloads"
fi

echo ""
echo "�� OPTIMIZATION TIPS:"
echo "================================"
echo "• Close unnecessary applications to free up RAM"
echo "• Use SSD storage for better I/O performance"
echo "• Consider using Docker Compose v2 for better resource management"
echo "• Monitor with: docker stats"
echo "• Check logs with: docker-compose logs -f [service_name]"


"""
echo ""                                                                                                      <....
🐳 Docker Compose Resource Check
================================

💾 MEMORY:
Total RAM: 62Gi
Available RAM: 58Gi
Used RAM: 3.2Gi
Free RAM: 56Gi

🖥️  CPU:
CPU Cores: 24
CPU Model: AMD Ryzen 9 3900X 12-Core Processor
Load Average:  1.09, 0.81, 0.49

💿 DISK:
Disk Usage:
Filesystem                                Size  Used Avail Use% Mounted on
/dev/sdd                                 1007G  435G  522G  46% /
/dev/sdf                                 1007G   57M  956G   1% /mnt/wsl/docker-desktop/docker-desktop-user-distro
/dev/loop0                                614M  614M     0 100% /mnt/wsl/docker-desktop/cli-tools

🐳 DOCKER:
Docker Version: Docker version 24.0.2, build cb74dfc
Docker Compose Version: docker-compose version 1.25.0, build unknown
Running Containers: 5

�� RECOMMENDATIONS:
================================
✅ RAM: Excellent! You have 58GB available
   → Can allocate: DeepHaven: 24GB, ClickHouse: 8GB, RedPanda: 4GB
✅ CPU: Excellent! You have 24 cores
   → Can allocate: DeepHaven: 4, ClickHouse: 2, RedPanda: 2

�� OPTIMIZATION TIPS:
================================
• Close unnecessary applications to free up RAM
• Use SSD storage for better I/O performance
• Consider using Docker Compose v2 for better resource management
• Monitor with: docker stats
• Check logs with: docker-compose logs -f [service_name]
"""