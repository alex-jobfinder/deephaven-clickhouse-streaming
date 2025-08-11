# DeepHaven ClickHouse Streaming Makefile
# Usage: make <target>

# Variables
COMPOSE_FILE := docker-compose.yaml
PROJECT_NAME := deephaven-clickhouse-streaming
LOGS_DIR := logs

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
PURPLE := \033[0;35m
CYAN := \033[0;36m
NC := \033[0m # No Color

# Default target
.DEFAULT_GOAL := help

# Help target
.PHONY: help
help: ## Show this help message
	@echo "$(CYAN)DeepHaven ClickHouse Streaming - Available Commands$(NC)"
	@echo "$(YELLOW)=================================================$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(YELLOW)Quick Start:$(NC)"
	@echo "  make start          # Start all services in background"
	@echo "  make logs           # Monitor all logs in real-time"
	@echo "  make stop           # Stop all services"
	@echo ""

# =============================================================================
# BASIC OPERATIONS
# =============================================================================

.PHONY: start
start: ## Start all services in background
	@echo "$(GREEN)Starting all services...$(NC)"
	docker compose up -d
	@echo "$(GREEN)Services started!$(NC)"
	@echo "$(CYAN)DeepHaven UI: http://localhost:10000/ide/$(NC)"
	@echo "$(CYAN)ClickHouse Play: http://localhost:8123/play$(NC)"
	@echo "$(CYAN)RedPanda Console: http://localhost:8081/overview$(NC)"

.PHONY: start-fg
start-fg: ## Start all services in foreground (see logs in real-time)
	@echo "$(GREEN)Starting all services in foreground...$(NC)"
	docker compose up

.PHONY: stop
stop: ## Stop all services
	@echo "$(YELLOW)Stopping all services...$(NC)"
	docker compose down
	@echo "$(GREEN)Services stopped!$(NC)"

.PHONY: restart
restart: ## Restart all services
	@echo "$(YELLOW)Restarting all services...$(NC)"
	docker compose restart
	@echo "$(GREEN)Services restarted!$(NC)"

.PHONY: rebuild
rebuild: ## Rebuild all images (use when you've made code changes)
	@echo "$(YELLOW)Rebuilding all images...$(NC)"
	docker compose down
	docker compose build --no-cache
	@echo "$(GREEN)Images rebuilt!$(NC)"

.PHONY: clean
clean: ## Stop and remove all containers, networks, and volumes
	@echo "$(RED)Cleaning up everything...$(NC)"
	docker compose down -v --remove-orphans
	@echo "$(GREEN)Cleanup complete!$(NC)"

# =============================================================================
# LOGGING COMMANDS
# =============================================================================

.PHONY: logs
logs: ## Monitor all services logs in real-time
	@echo "$(CYAN)Monitoring all service logs...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop monitoring$(NC)"
	docker compose logs -f

.PHONY: logs-cryptofeed
logs-cryptofeed: ## Monitor cryptofeed logs in real-time
	@echo "$(CYAN)Monitoring cryptofeed logs...$(NC)"
	docker compose logs -f cryptofeed

.PHONY: logs-deephaven
logs-deephaven: ## Monitor deephaven logs in real-time
	@echo "$(CYAN)Monitoring deephaven logs...$(NC)"
	docker compose logs -f deephaven

.PHONY: logs-clickhouse
logs-clickhouse: ## Monitor clickhouse logs in real-time
	@echo "$(CYAN)Monitoring clickhouse logs...$(NC)"
	docker compose logs -f clickhouse

.PHONY: logs-redpanda
logs-redpanda: ## Monitor redpanda logs in real-time
	@echo "$(CYAN)Monitoring redpanda logs...$(NC)"
	docker compose logs -f redpanda

.PHONY: logs-timestamp
logs-timestamp: ## Monitor all logs with timestamps
	@echo "$(CYAN)Monitoring all logs with timestamps...$(NC)"
	docker compose logs -f --timestamps

.PHONY: logs-recent
logs-recent: ## Show recent logs from all services
	@echo "$(CYAN)Recent logs from all services:$(NC)"
	docker compose logs --tail=100

.PHONY: logs-errors
logs-errors: ## Check for errors across all services
	@echo "$(RED)Checking for errors across all services:$(NC)"
	docker compose logs | grep -i "error\|exception\|failed\|warning" || echo "$(GREEN)No errors found!$(NC)"

.PHONY: logs-export
logs-export: ## Export logs to file with timestamp
	@echo "$(CYAN)Exporting logs...$(NC)"
	@mkdir -p $(LOGS_DIR)/combined
	docker compose logs > $(LOGS_DIR)/combined/export_$(shell date +%Y%m%d_%H%M%S).log
	@echo "$(GREEN)Logs exported to $(LOGS_DIR)/combined/$(NC)"

# =============================================================================
# SERVICE MANAGEMENT
# =============================================================================

.PHONY: status
status: ## Check service status
	@echo "$(CYAN)Service Status:$(NC)"
	docker compose ps

.PHONY: health
health: ## Check service health
	@echo "$(CYAN)Service Health:$(NC)"
	docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Health}}"

.PHONY: stats
stats: ## View resource usage
	@echo "$(CYAN)Resource Usage:$(NC)"
	docker stats --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

.PHONY: restart-cryptofeed
restart-cryptofeed: ## Restart cryptofeed service
	@echo "$(YELLOW)Restarting cryptofeed...$(NC)"
	docker compose restart cryptofeed
	@echo "$(GREEN)Cryptofeed restarted!$(NC)"

.PHONY: restart-deephaven
restart-deephaven: ## Restart deephaven service
	@echo "$(YELLOW)Restarting deephaven...$(NC)"
	docker compose restart deephaven
	@echo "$(GREEN)Deephaven restarted!$(NC)"

.PHONY: restart-clickhouse
restart-clickhouse: ## Restart clickhouse service
	@echo "$(YELLOW)Restarting clickhouse...$(NC)"
	docker compose restart clickhouse
	@echo "$(GREEN)Clickhouse restarted!$(NC)"

# =============================================================================
# DEBUGGING & TROUBLESHOOTING
# =============================================================================

.PHONY: shell-cryptofeed
shell-cryptofeed: ## Open bash shell in cryptofeed container
	@echo "$(CYAN)Opening shell in cryptofeed container...$(NC)"
	docker compose exec cryptofeed bash

.PHONY: shell-deephaven
shell-deephaven: ## Open bash shell in deephaven container
	@echo "$(CYAN)Opening shell in deephaven container...$(NC)"
	docker compose exec deephaven bash

.PHONY: shell-clickhouse
shell-clickhouse: ## Open bash shell in clickhouse container
	@echo "$(CYAN)Opening shell in clickhouse container...$(NC)"
	docker compose exec clickhouse bash

.PHONY: inspect-cryptofeed
inspect-cryptofeed: ## Inspect cryptofeed container details
	@echo "$(CYAN)Inspecting cryptofeed container...$(NC)"
	docker inspect cryptofeed

.PHONY: env-cryptofeed
env-cryptofeed: ## Show cryptofeed container environment variables
	@echo "$(CYAN)Cryptofeed environment variables:$(NC)"
	docker compose exec cryptofeed env

.PHONY: ping-test
ping-test: ## Test network connectivity between containers
	@echo "$(CYAN)Testing network connectivity...$(NC)"
	@echo "$(YELLOW)Testing cryptofeed -> redpanda...$(NC)"
	docker compose exec cryptofeed ping -c 3 redpanda || echo "$(RED)Failed$(NC)"
	@echo "$(YELLOW)Testing deephaven -> clickhouse...$(NC)"
	docker compose exec deephaven ping -c 3 clickhouse || echo "$(RED)Failed$(NC)"

# =============================================================================
# LOG MANAGEMENT
# =============================================================================

.PHONY: logs-setup
logs-setup: ## Create log directories
	@echo "$(CYAN)Creating log directories...$(NC)"
	mkdir -p $(LOGS_DIR)/{redpanda,deephaven,clickhouse,cryptofeed,combined}
	@echo "$(GREEN)Log directories created!$(NC)"

.PHONY: logs-size
logs-size: ## View log file sizes
	@echo "$(CYAN)Log file sizes:$(NC)"
	@du -h $(LOGS_DIR)/*/*.log 2>/dev/null | sort -hr | head -10 || echo "$(YELLOW)No log files found$(NC)"

.PHONY: logs-clean
logs-clean: ## Clean old logs (keep last 7 days)
	@echo "$(YELLOW)Cleaning old logs (keeping last 7 days)...$(NC)"
	find $(LOGS_DIR)/ -name "*.log" -mtime +7 -delete
	@echo "$(GREEN)Old logs cleaned!$(NC)"

.PHONY: logs-archive
logs-archive: ## Archive logs monthly
	@echo "$(CYAN)Archiving logs...$(NC)"
	@tar -czf $(LOGS_DIR)/archive_$(shell date +%Y%m).tar.gz $(LOGS_DIR)/*/$(shell date +%Y)*.log 2>/dev/null || echo "$(YELLOW)No logs to archive$(NC)"
	@echo "$(GREEN)Logs archived!$(NC)"

# =============================================================================
# SYSTEM MAINTENANCE
# =============================================================================

.PHONY: prune
prune: ## Clean up old containers and images
	@echo "$(YELLOW)Cleaning up old containers and images...$(NC)"
	docker system prune -f
	@echo "$(GREEN)Cleanup complete!$(NC)"

.PHONY: prune-all
prune-all: ## Full system cleanup (⚠️ careful - removes data)
	@echo "$(RED)⚠️  FULL SYSTEM CLEANUP - This will remove ALL unused data!$(NC)"
	@read -p "Are you sure? Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ] || exit 1
	docker system prune -a -f --volumes
	@echo "$(GREEN)Full cleanup complete!$(NC)"

.PHONY: update
update: ## Update all images to latest versions
	@echo "$(YELLOW)Updating all images...$(NC)"
	docker compose pull
	@echo "$(GREEN)Images updated!$(NC)"

# =============================================================================
# MONITORING DASHBOARD
# =============================================================================

.PHONY: monitor
monitor: ## Show monitoring dashboard (status + recent logs)
	@echo "$(CYAN)=== MONITORING DASHBOARD ===$(NC)"
	@echo ""
	@echo "$(YELLOW)Service Status:$(NC)"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Health}}"
	@echo ""
	@echo "$(YELLOW)Recent Logs (last 20 lines):$(NC)"
	@docker compose logs --tail=20
	@echo ""
	@echo "$(YELLOW)Resource Usage:$(NC)"
	@docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

.PHONY: watch
watch: ## Watch service status and logs in real-time
	@echo "$(CYAN)Watching services in real-time...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	watch -n 5 'make monitor'

# =============================================================================
# DEVELOPMENT TOOLS
# =============================================================================

.PHONY: test-hyperliquid
test-hyperliquid: ## Test HyperLiquid connection
	@echo "$(CYAN)Testing HyperLiquid connection...$(NC)"
	docker compose exec cryptofeed python3 /cryptofeed/src/test_hyperliquid.py

.PHONY: test-cryptofeed
test-cryptofeed: ## Test cryptofeed trades
	@echo "$(CYAN)Testing cryptofeed trades...$(NC)"
	docker compose exec cryptofeed python3 /cryptofeed/src/script/cryptofeed_1_trades.py

.PHONY: test-orderbooks
test-orderbooks: ## Test cryptofeed orderbooks
	@echo "$(CYAN)Testing cryptofeed orderbooks...$(NC)"
	docker compose exec cryptofeed python3 /cryptofeed/src/script/cryptofeed_2_orderbooks.py

# =============================================================================
# UTILITY COMMANDS
# =============================================================================

.PHONY: version
version: ## Show Docker and Docker Compose versions
	@echo "$(CYAN)Docker Version:$(NC)"
	docker --version
	@echo "$(CYAN)Docker Compose Version:$(NC)"
	docker compose version

.PHONY: info
info: ## Show project information
	@echo "$(CYAN)=== PROJECT INFORMATION ===$(NC)"
	@echo "$(GREEN)Project:$(NC) $(PROJECT_NAME)"
	@echo "$(GREEN)Compose File:$(NC) $(COMPOSE_FILE)"
	@echo "$(GREEN)Logs Directory:$(NC) $(LOGS_DIR)"
	@echo ""
	@echo "$(YELLOW)Services:$(NC)"
	@docker compose config --services | sort
	@echo ""
	@echo "$(YELLOW)Ports:$(NC)"
	@echo "  DeepHaven UI: http://localhost:10000/ide/"
	@echo "  ClickHouse Play: http://localhost:8123/play"
	@echo "  RedPanda Console: http://localhost:8081/overview"

.PHONY: backup
backup: ## Create backup of data and logs
	@echo "$(CYAN)Creating backup...$(NC)"
	@mkdir -p backups
	@tar -czf backups/backup_$(shell date +%Y%m%d_%H%M%S).tar.gz data/ logs/ 2>/dev/null || echo "$(YELLOW)No data/logs to backup$(NC)"
	@echo "$(GREEN)Backup created!$(NC)"

# =============================================================================
# QUICK WORKFLOWS
# =============================================================================

.PHONY: dev
dev: ## Development workflow: start services and monitor logs
	@echo "$(GREEN)Starting development workflow...$(NC)"
	@make start
	@echo "$(YELLOW)Waiting for services to be ready...$(NC)"
	@sleep 10
	@make logs

.PHONY: debug
debug: ## Debug workflow: restart cryptofeed and monitor logs
	@echo "$(YELLOW)Starting debug workflow...$(NC)"
	@make restart-cryptofeed
	@echo "$(YELLOW)Waiting for cryptofeed to restart...$(NC)"
	@sleep 5
	@make logs-cryptofeed

.PHONY: reset
reset: ## Reset workflow: clean restart of all services
	@echo "$(RED)Starting reset workflow...$(NC)"
	@make stop
	@echo "$(YELLOW)Waiting for services to stop...$(NC)"
	@sleep 5
	@make start
	@echo "$(GREEN)Reset complete!$(NC)"

# =============================================================================
# SPECIAL TARGETS
# =============================================================================

.PHONY: all
all: ## Build and start everything
	@echo "$(GREEN)Building and starting everything...$(NC)"
	@make rebuild
	@make start

.PHONY: logs-all
logs-all: ## Show logs from all services (not following)
	@echo "$(CYAN)All service logs:$(NC)"
	docker compose logs

.PHONY: check
check: ## Health check all services
	@echo "$(CYAN)Performing health checks...$(NC)"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Health}}" | grep -v "healthy" || echo "$(GREEN)All services are healthy!$(NC)"