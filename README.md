# Setup Guide for DuckDB with Performance Monitoring

Follow these steps to set up and run the DuckDB Billion Row Challenge with comprehensive performance monitoring:

## 1. Create Project Structure

```bash
mkdir -p duckdb-challenge
cd duckdb-challenge
mkdir -p data scripts
```

## 2. Save Required Files

Save the following files to your project directory:

- `Dockerfile` (in the root directory)
- `docker-compose.yml` (in the root directory)
- `billion_row_challenge.py` (in the root directory)
- `performance_monitor.py` (in the root directory)

## 3. Add Required Dependencies

The `docker-compose.yml` file is configured to automatically install the Python packages needed for monitoring:
- matplotlib
- pandas
- psutil

## 4. Build and Run the Container

```bash
docker-compose up --build
```

This will:
1. Build the Docker image
2. Install the necessary dependencies
3. Copy the monitoring script to the scripts directory
4. Run the billion row challenge with monitoring enabled

## 5. View the Performance Reports

After the script completes, you'll find performance reports in:

```
./data/performance/report/report.html
```

You can open this HTML file in any web browser to see detailed performance metrics, including:
- CPU usage over time
- Memory usage over time
- Disk I/O patterns
- Query execution times
- Event timeline

## 6. Adjusting Parameters

You can customize the following parameters:

### Row Count
```bash
ROW_COUNT=100000000 docker-compose up
```

### Memory and CPU Allocation
Edit `docker-compose.yml`:
```yaml
mem_limit: 32g  # Increase for larger datasets
cpus: 8.0       # Increase for better performance
```

### Monitoring Sample Rate
In `billion_row_challenge.py`, adjust:
```python
monitor.start_monitoring(interval=0.5)  # Sample every 0.5 seconds
```

## 7. Performance Optimization Tips

Based on the monitoring results, you can:

1. Adjust DuckDB memory settings
2. Change chunk sizes for better performance
3. Identify bottlenecks in query execution
4. Optimize data loading parameters

## 8. Troubleshooting

### Missing Dependencies
If you encounter errors related to missing Python packages, you can manually install them:

```bash
docker exec -it duckdb-billion-row pip install matplotlib pandas psutil
```

### Performance Report Not Generated
If performance reports aren't generated, check:
- Directory permissions
- Python error messages in the console
- Manually run the report generation:

```bash
docker exec -it duckdb-billion-row python3 -c "
from scripts.performance_monitor import PerformanceMonitor
monitor = PerformanceMonitor(output_dir='/app/data/performance')
monitor.generate_report('/app/data/performance/duckdb_perf_YYYYMMDD_HHMMSS.json')
"
```

Replace the filename with your actual metrics file.