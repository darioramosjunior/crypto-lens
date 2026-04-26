# 🔍 Crypto-Lens

A comprehensive cryptocurrency intelligence platform that monitors, analyzes, and alerts on crypto market movements in real-time. This data engineering project combines live market data collection, advanced technical analysis, and intelligent alerting to provide actionable insights into cryptocurrency markets.

Grafana
<img width="1560" height="742" alt="image" src="https://github.com/user-attachments/assets/fa73aa08-82ce-48c4-8e05-f0c800895807" />

Discord Alerts
<img width="836" height="811" alt="image" src="https://github.com/user-attachments/assets/bdeeb192-50a9-405e-b0aa-ddba5a64817a" />
<img width="1056" height="765" alt="image" src="https://github.com/user-attachments/assets/4d96785a-2c71-46af-9e68-fa6341952434" />
<img width="848" height="712" alt="image" src="https://github.com/user-attachments/assets/3480e148-1ff5-493a-8e87-7a3774287065" />

## 📋 Overview

Crypto-Lens is a production-ready data pipeline that:
- **Monitors** cryptocurrency markets 24/7 across multiple timeframes
- **Analyzes** market data with technical indicators (RSI, moving averages, sentiment analysis)
- **Screens** for significant open interest changes on futures markets
- **Visualizes** market trends and metrics in real-time dashboards (Grafana)
- **Alerts** traders instantly via Discord webhooks on market conditions and anomalies
- **Stores** latest market data in AWS S3 for real-time monitoring and alerting

Perfect for traders, data analysts, and crypto enthusiasts who want automated, data-driven market intelligence.

## 🚀 Key Features

### Core Monitoring Capabilities
- **Hourly Market Pulse** - Real-time market analysis updated every hour
  - Price movements across all cryptocurrencies
  - RSI sentiment analysis with visualizations
  - Trend identification for each coin & sentiment analysis
  - Automated Discord alerts with trend charts

- **Daily Market Analysis** - Comprehensive daily market summaries
  - 24-hour price action and technical analysis
  - Latest Top Gainers & Top Losers for the day

- **Market Breadth Analysis** - Aggregate market metrics and trends
  - Analyzes market-wide sentiment across all trading pairs
  - Continues advancing/declining coin counts

- **Open Interest Screener** - Futures market intelligence
  - Monitors OI changes across all trading pairs
  - Alerts on significant position accumulations
  - Tracks top 20 movers in OI percentage change

- **Coin Data Collector** - Metadata and fundamental analysis
  - Fetches market cap data from CoinMarketCap API
  - Categorizes market cap value for each trading pair

- **Pipeline Observability** - Real-time pipeline monitoring
  - Monitors all data pipeline scripts for errors/warnings
  - Analyzes log files for anomalies and failures
  - Sends aggregated reports to Discord
  - Tracks pipeline health and performance metrics

### Data & Visualization
- **Multiple Data Sources**
  - Binance Futures APIs (via CCXT)
  - CoinMarketCap API for market fundamentals
  - Real-time WebSocket connections for live updates

- **Grafana Dashboards**
  - Real-time market visualization
  - Custom panels for daily & hourly sentiment analysis
  - Real-time identification of top coins in price change & open interest

- **Persistent Storage**
  - Local CSV exports for analysis
  - AWS S3 backup of latest datasets for redundancy

### Alerting & Integration
- **Discord Webhooks**
  - Embedded market charts and visualizations
  - Real-time notifications with actionable insights
  - Flexible webhook configuration for any webhook-compatible service

- **Structured Logging**
  - Comprehensive event logging for all operations
  - Automatic log cleanup
  - Audit trail for compliance

## 📊 Architecture

### Data Pipeline Flow
```
Market Data Sources (Binance, CMC) 
    ↓
Async Data Fetchers (Hourly/Daily/OI Screener)
    ↓
Technical Analysis & Anomaly Detection
    ↓
Visualization & Reporting
    ↓
Discord Alerts & S3 Archival
```

### Components

| Script | Purpose | Frequency | Output |
|--------|---------|-----------|--------|
| `main.py` | Script orchestrator | Every 5 minutes (cron) | Orchestrates all sub-scripts in sequence |
| `hourly_fetch_and_pulse.py` | Market pulse analysis | Hourly (via main.py) | Charts, Discord alerts, CSV data |
| `daily_fetch_and_pulse.py` | Daily market summary | Daily (via main.py) | Performance reports, visualizations, S3 upload |
| `market_breadth.py` | Market breadth metrics | Hourly (via main.py) | Breadth data, sentiment reports, S3 upload |
| `oi_change_screener.py` | Open interest monitoring | Hourly (via main.py) | OI change alerts, top 20 movers |
| `coin_data_collector.py` | Coin metadata collection | Daily (via main.py) | Coin database CSV, market cap data |
| `pipeline_observability.py` | Pipeline health monitoring | Hourly (via main.py) | Error/warning summaries, Discord alerts |
| `discord_integrator.py` | Webhook integration | On-demand | Message/image delivery to Discord |
| `logger.py` | Event logging system | Continuous | Structured logs with timestamps |
| `logs_cleaner.py` | Log maintenance | Daily (separate cron) | Cleanup of logs older than retention period |

## 🛠️ Tech Stack

### Core Dependencies
- **Data Processing**: Pandas, NumPy, SciPy
- **Technical Analysis**: Pandas-TA (RSI, Moving Averages, etc.)
- **Data Validation**: Pydantic (runtime type validation)
- **Market Data**: CCXT, Binance Futures API, aiohttp
- **APIs**: CoinMarketCap, Discord Webhooks
- **Visualization**: Matplotlib, Kaleido (chart export)
- **Cloud Storage**: AWS S3 (boto3)
- **Async Processing**: Python asyncio
- **Time Series**: Dateparser
- **Testing**: Pytest, Flake8 (linting)

### Optional Integrations
- **Grafana** - For dashboard visualization
- **Any Webhook Service** - Discord, Slack, etc.
- **AWS S3** - For data archival
- **GitHub Actions** - For CI/CD pipeline testing

## 📦 Requirements

```
Python 3.8+
See requirements.txt for full dependency list
```

Key packages:
- ccxt==4.4.78
- pandas==2.2.3
- numpy>=2.0.0
- pandas-ta (technical analysis & advanced indicators)
- pydantic (data validation)
- aiohttp==3.10.11
- boto3 (for AWS S3)
- requests (for webhooks)
- matplotlib>=3.9.0
- pytest (testing framework)
- flake8 (code linting)

## 🚀 Getting Started

### Prerequisites
- Python 3.8 or higher
- API Keys (optional but recommended):
  - **CoinMarketCap API Key** - For market cap data
  - **Discord Webhook URL** - For alerts
  - **AWS Credentials** - For S3 storage

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/crypto-lens.git
   cd crypto-lens
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   Create a `.env` file in the project root:
   ```env
   # Discord Webhooks
   MARKET_PULSE_WEBHOOK=https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN
   DAILY_PULSE_WEBHOOK=https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN
   OI_SCREENER_WEBHOOK=https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN
   
   # CoinMarketCap API
   cmc_api_key=YOUR_CMC_API_KEY
   
   # AWS Configuration
   AWS_ACCESS_KEY_ID=YOUR_AWS_KEY
   AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET
   AWS_REGION=ap-southeast-2
   ```

5. **Configure application settings**
   Edit `config.conf` to customize:
   - Log and output paths
   - Cron schedules for automated runs
   - Data retention policies

### Running the Application

**Manual execution:**
```bash
python main.py
```

**Individual scripts:**
```bash
python hourly_fetch_and_pulse.py
python daily_fetch_and_pulse.py
python oi_change_screener.py
python coin_data_collector.py
```

**Automated execution (Linux/Unix - Recommended):**

The `setup.sh` script configures the application for automated execution on EC2 or Linux systems:

```bash
sudo ./setup.sh
```

This automatically creates:
- **Cron jobs** for scheduled execution:
  - `main.py` - Runs every 5 minutes, orchestrating all analysis scripts
  - `logs_cleaner.py` - Runs daily at 3 PM to clean old log files
  
- **Systemd service** (`crypto-lens-init`):
  - Runs on boot to ensure all directories and permissions are correct
  - Recreates `/var/run/crypto-lens/` after system reboots (tmpfs cleanup)
  - Fixes log file permissions in `/var/log/crypto-lens/`
  
- **Systemd tmpfiles.d configuration**:
  - Ensures `/var/run/crypto-lens/` is recreated with proper permissions on every reboot
  - Maintains service operational file directory even after system restart

**Manual cron setup (if not using setup.sh):**
```bash
# Add to crontab for continuous monitoring
# Run main script orchestrator every 5 minutes (executes all analysis scripts)
*/5 * * * * cd /path/to/crypto-lens && /path/to/venv/bin/python3 main.py >> /var/log/crypto-lens/main.log 2>&1

# Clean logs daily at 3 PM
0 15 * * * cd /path/to/crypto-lens && /path/to/venv/bin/python3 logs_cleaner.py >> /var/log/crypto-lens/logs_cleaner.log 2>&1
```

## 🧪 Testing & Quality Assurance

The project includes comprehensive testing infrastructure:

### Unit Tests
- Located in `tests/` directory
- Covers critical modules:
  - `test_coin_data_collector.py` - Coin collection logic
  - `test_utils.py` - Utility helper functions
  - `test_validations.py` - Data validation models
  - `test_logger_config.py` - Logging configuration
  - Integration tests for data pipelines

### Running Tests
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_coin_data_collector.py

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=. tests/
```

### Running Tests with Coverage
```bash
# Install coverage package
pip install pytest-cov

# Run tests with coverage
pytest --cov=. --cov-report=html tests/
```

### Continuous Integration
- **GitHub Actions** - Automated testing on push/PR
- **Linting** - Flake8 checks for code quality (E9, F63, F7, F82)
- **Multi-version testing** - Tests run on Python 3.10 and 3.11
- **Pre-commit hooks** - Setup available via `setup_pre_commit.py`

### Data Validation
The project uses **Pydantic** for runtime data validation:
- **CoinDataModel** - Validates coin metadata
- **OHLCVData** - Validates candlestick data
- **TrendCounts** - Validates trend analysis results
- **PriceChangeData** - Validates price change metrics
- All data collectors validate output before writing/sending

For detailed testing information, see [TESTING_QUICKSTART.md](TESTING_QUICKSTART.md)

## � Directory & Permission Persistence

The application is configured to maintain directory structure and permissions across EC2 instance reboots:

### Dynamic Runtime Directory (`/var/run/crypto-lens/`)
On Linux/Unix systems, `/var/run/` is a temporary filesystem (tmpfs) that gets cleared on reboot. The setup script handles this automatically:

- **systemd tmpfiles.d** (`/etc/tmpfiles.d/crypto-lens.conf`)
  - Recreates `/var/run/crypto-lens/` on every boot
  - Ensures proper ownership (`crypto-lens:crypto-lens`) and permissions (`755`)
  - Runs during systemd initialization, before services start

- **crypto-lens-init Service** (`/etc/systemd/system/crypto-lens-init.service`)
  - Runs on boot before cron services
  - Double-checks directory existence and permissions
  - Fixes any file permissions in both runtime and log directories
  - Ensures application can write output immediately after reboot

### Log Directory (`/var/log/crypto-lens/`)
- Persists across reboots (stored on persistent filesystem)
- Ownership and permissions restored by `crypto-lens-init` service on boot
- Old log files automatically cleaned by `logs_cleaner.py` based on retention policy

### Verification After Reboot
To verify persistence is working after an EC2 restart:
```bash
# Check runtime directory exists
ls -la /var/run/crypto-lens/

# Check log directory exists
ls -la /var/log/crypto-lens/

# Verify service status
systemctl status crypto-lens-init

# View tmpfiles.d configuration
cat /etc/tmpfiles.d/crypto-lens.conf
```

## �📊 Output & Visualization

### Generated Outputs
- **CSV Data Files**:
  - `coin_data.csv` - Master cryptocurrency dataset
  - `prices_1h.csv` - Hourly price data
  - `prices_1d.csv` - Daily price data
  - `coin_trend_1h.csv` - Technical indicators (hourly)
  - `coin_trend_1d.csv` - Technical indicators (daily)
  - `oi_changes_1h.csv` - Open interest changes

- **Visualization Charts**:
  - `market_pulse.png` - Hourly market sentiment visualization
  - `rsi_sentiment.png` - RSI analysis heatmap
  - `market_pulse_daily.png` - Daily market summary

### Grafana Dashboards

Connect Grafana to your data source (S3 or local database) to visualize:
- Real-time market trends across multiple timeframes
- Technical indicator distributions (RSI, MA crossovers)
- Open interest accumulation patterns
- Volume and volatility metrics
- Historical performance analysis

Example dashboard setup:
1. Configure Grafana data source pointing to S3 or time-series database
2. Import dashboards or create custom panels
3. Set refresh intervals to match script execution frequency
4. Customize alerts based on technical conditions

## 🎯 Code Quality & Architecture

### Type Hints & Code Safety
- **Full Type Annotations** - All modules include comprehensive type hints for better IDE support and error detection
- **Static Analysis** - Flake8 linting ensures code quality standards
- **Runtime Validation** - Pydantic models validate all data throughout the pipeline

### Shared Utilities (`utils.py`)
Centralized helper functions to reduce code duplication:
- **FileUtility** - File and directory operations
- **ConfigManager** - Configuration value retrieval
- **S3Manager** - AWS S3 upload/download operations
- **DataProcessor** - DataFrame transformations and calculations
- **TechnicalIndicators** - Pandas-TA wrapper functions

### Data Validation Pipeline
Real-time validation using Pydantic models:
- Validates coin data (symbols, market caps)
- Validates OHLCV candlestick data
- Validates technical indicators and calculations
- Validates price change metrics
- Ensures data integrity before processing

### Production-Ready Features
- **Comprehensive Logging** - Structured logs for all operations
- **Error Handling** - Graceful error recovery and alerts
- **Async Processing** - Efficient concurrent API requests
- **Rate Limiting** - Respects API rate limits
- **Data Persistence** - Maintains state across runs

## 📝 Configuration

### config.conf Format
```ini
[paths]
log_path = /var/log/crypto-lens/
output_path = /var/run/crypto-lens/

[schedules]
main_cron_sched = */5 * * * *
logs_cleaner_cron_sched = 0 15 * * *
```

### Key Configuration Options
- **Log Path**: Directory for application logs
- **Output Path**: Directory for CSV and image outputs (dynamic runtime directory)
- **Cron Schedules**: Frequency of automated runs (see crontab syntax)
  - `main_cron_sched`: Frequency for main.py (orchestrates all analysis scripts)
  - `logs_cleaner_cron_sched`: Frequency for log cleanup
  - Note: All analysis scripts (hourly, daily, OI screener, coin collector, market breadth, pipeline observability) run through main.py
- **Webhook URLs**: Discord or compatible webhook endpoints
- **AWS Region**: For S3 storage operations

## 🔍 How It Works

### Hourly Market Pulse
1. Fetches OHLCV data for major cryptocurrencies
2. Calculates technical indicators (RSI, moving averages)
3. Performs sentiment analysis (bullish/bearish)
4. Generates market pulse visualization
5. Sends alert to Discord with chart

### Daily Analysis
1. Aggregates daily price data
2. Analyzes multi-timeframe trends (1h, 4h, 1d, 1w)
3. Identifies support/resistance levels
4. Generates daily performance report
5. Backs up latest data to AWS S3

### Market Breadth Analysis
1. Analyzes aggregate market metrics across all pairs
2. Calculates advance/decline statistics
3. Sends breadth reports to Discord and S3

### OI Change Screener
1. Monitors open interest across Binance pairs
2. Tracks percentage changes from previous scan
3. Identifies top 20 coins with highest OI changes
4. Alerts on significant accumulation/liquidation
5. Maintains historical OI data for analysis

### Coin Data Collection
1. Queries CoinMarketCap API for market fundamentals
2. Extracts market cap, volume, ranking data
3. Correlates with technical indicators
4. Updates master coin database
5. Enables fundamental + technical analysis

### Pipeline Observability
1. Monitors log files from all data pipeline scripts
2. Parses logs for errors, warnings, and anomalies
3. Aggregates pipeline health metrics
4. Generates summary report of latest run
5. Sends observability alerts to Discord webhook

## 📈 Use Cases

- **Momentum Trading** - Real-time alerts on trend shifts and OI changes
- **Risk Management** - Monitor large position movements and sentiment shifts
- **Market Research** - Analyze historical correlations and patterns
- **Portfolio Tracking** - Monitor holdings and key metrics in real-time
- **Data Analysis** - Pull data for backtesting and ML model training

## 🔐 Security & Best Practices

- **Never commit .env file** - Keep API keys and webhooks private
- **Use environment variables** - For all sensitive configuration
- **Enable log rotation** - Prevent disk space issues in production
- **Monitor logs regularly** - Watch for errors and anomalies
- **Update dependencies** - Keep libraries current for security patches
- **Rate limiting** - Scripts respect API rate limits automatically
- **Error handling** - Comprehensive error logging and graceful degradation

## 📊 Data Retention & Storage

- **Local Logs**: Automatically cleaned per retention policy
- **CSV Data**: Maintained locally for current analysis
- **S3 Backup**: Latest datasets stored for redundancy and real-time monitoring
- **Database**: Optional integration with InfluxDB or TimescaleDB

## 🤝 Integration Capabilities

### Webhook Services
The Discord integration can be adapted to work with any webhook-compatible service:
- Slack (via Incoming Webhooks)
- Microsoft Teams (via Connectors)
- Custom APIs supporting JSON POST requests
- IFTTT and others

Simply modify the webhook URL in `.env` to use alternative services.

### Data Warehouse
For historical analysis beyond alerting/monitoring, data can be exported to:
- PostgreSQL / TimescaleDB
- InfluxDB (time-series)
- Elasticsearch
- Data Lakes (S3, GCS, Azure Blob)

## 🐛 Troubleshooting

### Common Issues

**Directories/permissions missing after EC2 reboot**
- Solution: Ensure `setup.sh` was run and the `crypto-lens-init` service is enabled
- Verify: `systemctl status crypto-lens-init` and `systemctl is-enabled crypto-lens-init`
- Check: `/etc/tmpfiles.d/crypto-lens.conf` exists and has correct content

**"MARKET_PULSE_WEBHOOK not set"**
- Solution: Add webhook URL to `.env` file as shown in configuration section

**"API Rate Limit Exceeded"**
- Solution: Scripts have built-in rate limiting; may need to add delays or increase API tier

**"Connection timeout on aiohttp"**
- Solution: Check internet connection and API endpoint availability
- Verify AWS credentials for S3 access

**"CoinMarketCap data not collected"**
- Solution: Verify `cmc_api_key` in .env; check CMC account quota

## 📝 Logging

All operations are logged to:
- **hourly_fetch_and_pulse.log** - Hourly market analysis
- **daily_fetch_and_pulse.log** - Daily analysis
- **oi_change_screener.log** - OI screening operations
- **coin_data_collector.log** - Data collection events
- **discord_integrator.log** - Webhook delivery logs

View logs:
```bash
tail -f /var/log/crypto-lens/hourly_fetch_and_pulse.log
```

## 🚀 Performance Considerations

- **Async/Await** - Maximizes throughput for API requests
- **Batch Processing** - Efficient data processing with Pandas
- **Caching** - Reuses computed values where appropriate
- **Connection Pooling** - Reuses HTTP connections
- **Incremental Updates** - Only processes changed data

Typical execution times:
- Hourly script: 2-5 minutes
- Daily script: 3-10 minutes
- OI screener: 1-3 minutes
- Coin collector: 2-5 minutes

## 📚 Additional Resources

- [CCXT Exchange Library Documentation](https://github.com/ccxt/ccxt)
- [Binance API Documentation](https://binance-docs.github.io/apidocs/)
- [CoinMarketCap API Docs](https://coinmarketcap.com/api/documentation/v1/)
- [Pandas-TA Technical Analysis](https://github.com/twopirllc/pandas-ta)
- [Discord Webhook Integration](https://discord.com/developers/docs/resources/webhook)
- [Grafana Documentation](https://grafana.com/docs/)

## 📄 License

This project is provided as-is for educational and commercial use.

## 🎓 About

This project demonstrates:
- ✅ Real-time data pipeline architecture
- ✅ Async programming with Python (asyncio)
- ✅ Third-party API integration (REST, WebSocket)
- ✅ Technical indicator development with Pandas-TA
- ✅ Market-wide sentiment analysis (breadth metrics)
- ✅ Cloud data storage (AWS S3)
- ✅ Automated alerting systems
- ✅ Data visualization and dashboarding (Matplotlib, Grafana)
- ✅ Production-grade logging and error handling
- ✅ Scheduled task orchestration
- ✅ Regex and financial data processing
- ✅ Type hints and static analysis (Pylance, Flake8)
- ✅ Comprehensive unit testing (Pytest)
- ✅ Runtime data validation (Pydantic)
- ✅ Shared utility libraries and code organization
- ✅ CI/CD pipeline integration (GitHub Actions)

Perfect portfolio project showcasing full-stack data engineering capabilities with production best practices.

---

**For questions or support**, review the logs, check API credentials, and verify webhook connectivity.
