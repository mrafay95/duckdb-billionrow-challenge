# performance_monitor.py
# Add this script to your project for detailed DuckDB performance monitoring

import duckdb
import time
import os
import psutil
import json
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

class PerformanceMonitor:
    def __init__(self, output_dir="./performance_data"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.metrics = {
            "timestamp": [],
            "cpu_percent": [],
            "memory_percent": [],
            "memory_used_mb": [],
            "disk_read_mb": [],
            "disk_write_mb": [],
            "query_times_ms": {},
            "events": []
        }
        self.start_time = time.time()
        self.last_disk_io = psutil.disk_io_counters()
        self.last_disk_time = time.time()
        
        # Get DuckDB version and configuration
        conn = duckdb.connect(':memory:')
        self.duckdb_version = conn.execute("SELECT version()").fetchone()[0]
        self.system_info = {
            "os": os.name,
            "python_version": ".".join(map(str, os.sys.version_info[:3])),
            "cpu_count": os.cpu_count(),
            "total_memory_gb": psutil.virtual_memory().total / (1024**3),
            "duckdb_version": self.duckdb_version
        }
        conn.close()
        
        # Initialize monitoring
        self.monitoring = False
        self.monitor_interval = 1.0  # seconds
    
    def start_monitoring(self, interval=1.0):
        """Start background monitoring thread"""
        import threading
        self.monitor_interval = interval
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        self.record_event("Monitoring started")
        
    def stop_monitoring(self):
        """Stop background monitoring"""
        self.monitoring = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=2.0)
        self.record_event("Monitoring stopped")
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.monitoring:
            self.capture_point()
            time.sleep(self.monitor_interval)
    
    def capture_point(self):
        """Capture a single monitoring data point"""
        now = time.time()
        process = psutil.Process(os.getpid())
        
        # CPU and memory
        cpu_percent = psutil.cpu_percent(interval=0.1)  # Get system-wide CPU usage
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        # Disk I/O
        current_disk_io = psutil.disk_io_counters()
        io_time_diff = now - self.last_disk_time
        
        if io_time_diff > 0:
            read_diff = (current_disk_io.read_bytes - self.last_disk_io.read_bytes) / io_time_diff / (1024**2)
            write_diff = (current_disk_io.write_bytes - self.last_disk_io.write_bytes) / io_time_diff / (1024**2)
        else:
            read_diff = 0
            write_diff = 0
            
        self.last_disk_io = current_disk_io
        self.last_disk_time = now
        
        # Record metrics
        self.metrics["timestamp"].append(now - self.start_time)
        self.metrics["cpu_percent"].append(cpu_percent)
        self.metrics["memory_percent"].append(memory_percent)
        self.metrics["memory_used_mb"].append(memory_info.rss / (1024**2))
        self.metrics["disk_read_mb"].append(read_diff)
        self.metrics["disk_write_mb"].append(write_diff)
    
    def record_event(self, event_name):
        """Record a named event with timestamp"""
        now = time.time()
        self.metrics["events"].append({
            "time": now - self.start_time,
            "name": event_name
        })
        return now
    
    def record_query(self, query_name, conn, query_sql):
        """Execute a query and record its performance"""
        self.record_event(f"Query start: {query_name}")
        start_time = time.time()
        
        # Execute query and fetch results
        result = conn.execute(query_sql).fetchall()
        
        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000
        
        # Store in query times
        if query_name not in self.metrics["query_times_ms"]:
            self.metrics["query_times_ms"][query_name] = []
        
        self.metrics["query_times_ms"][query_name].append(duration_ms)
        self.record_event(f"Query end: {query_name} ({duration_ms:.2f}ms)")
        
        return result, duration_ms
    
    def save_metrics(self, filename=None):
        """Save collected metrics to a JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"duckdb_perf_{timestamp}.json"
        
        filepath = os.path.join(self.output_dir, filename)
        
        # Add system info to metrics
        output_data = {
            "system_info": self.system_info,
            "metrics": self.metrics,
        }
        
        with open(filepath, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"Performance metrics saved to {filepath}")
        return filepath
    
    def generate_report(self, metrics_file=None):
        """Generate performance report with charts"""
        if metrics_file is None:
            # Use the metrics in memory
            data = {
                "system_info": self.system_info,
                "metrics": self.metrics
            }
        else:
            # Load from file
            with open(metrics_file, 'r') as f:
                data = json.load(f)
        
        # Create report directory
        report_dir = os.path.join(self.output_dir, "report")
        os.makedirs(report_dir, exist_ok=True)
        
        # Generate plots
        self._plot_resource_usage(data, report_dir)
        self._plot_query_performance(data, report_dir)
        
        # Generate HTML report
        self._generate_html_report(data, report_dir)
        
        print(f"Performance report generated in {report_dir}")
        return report_dir
    
    def _plot_resource_usage(self, data, report_dir):
        """Create resource usage plots"""
        metrics = data["metrics"]
        timestamps = metrics["timestamp"]
        
        plt.figure(figsize=(12, 8))
        
        # CPU usage
        plt.subplot(3, 1, 1)
        plt.plot(timestamps, metrics["cpu_percent"])
        plt.title("CPU Usage")
        plt.ylabel("CPU %")
        plt.grid(True)
        
        # Memory usage
        plt.subplot(3, 1, 2)
        plt.plot(timestamps, metrics["memory_used_mb"])
        plt.title("Memory Usage")
        plt.ylabel("Memory (MB)")
        plt.grid(True)
        
        # Disk I/O
        plt.subplot(3, 1, 3)
        plt.plot(timestamps, metrics["disk_read_mb"], label="Read")
        plt.plot(timestamps, metrics["disk_write_mb"], label="Write")
        plt.title("Disk I/O")
        plt.ylabel("MB/s")
        plt.legend()
        plt.grid(True)
        plt.xlabel("Time (seconds)")
        
        # Mark events on all plots
        for event in metrics["events"]:
            if event["name"].startswith("Query"):
                for i in range(1, 4):
                    plt.subplot(3, 1, i)
                    plt.axvline(x=event["time"], color='r', linestyle='--', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(report_dir, "resource_usage.png"))
        plt.close()
    
    def _plot_query_performance(self, data, report_dir):
        """Create query performance plots"""
        query_times = data["metrics"]["query_times_ms"]
        
        if not query_times:
            return
            
        # Create query time comparison
        plt.figure(figsize=(10, 6))
        
        query_names = list(query_times.keys())
        avg_times = [sum(times)/len(times) for times in query_times.values()]
        
        bars = plt.bar(query_names, avg_times)
        plt.title("Query Performance")
        plt.ylabel("Execution Time (ms)")
        plt.xticks(rotation=45, ha="right")
        plt.grid(axis="y")
        
        # Add time labels on bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 5,
                    f'{height:.1f}',
                    ha='center', va='bottom', rotation=0)
        
        plt.tight_layout()
        plt.savefig(os.path.join(report_dir, "query_performance.png"))
        plt.close()
    
    def _generate_html_report(self, data, report_dir):
        """Generate HTML report with all metrics and charts"""
        system_info = data["system_info"]
        metrics = data["metrics"]
        
        # Calculate summary statistics
        avg_cpu = sum(metrics["cpu_percent"]) / len(metrics["cpu_percent"]) if metrics["cpu_percent"] else 0
        max_cpu = max(metrics["cpu_percent"]) if metrics["cpu_percent"] else 0
        avg_memory = sum(metrics["memory_used_mb"]) / len(metrics["memory_used_mb"]) if metrics["memory_used_mb"] else 0
        max_memory = max(metrics["memory_used_mb"]) if metrics["memory_used_mb"] else 0
        
        # Format query times
        query_summaries = []
        for query_name, times in metrics["query_times_ms"].items():
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            query_summaries.append({
                "name": query_name,
                "avg_ms": avg_time,
                "min_ms": min_time,
                "max_ms": max_time,
                "count": len(times)
            })
        
        # Create HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>DuckDB Performance Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1, h2 {{ color: #333; }}
                .section {{ margin-bottom: 30px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .chart-container {{ margin: 20px 0; }}
                .summary {{ background-color: #eef; padding: 15px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <h1>DuckDB Performance Report</h1>
            <div class="section summary">
                <h2>Summary</h2>
                <p>
                    <strong>Total duration:</strong> {metrics["timestamp"][-1]:.2f} seconds<br>
                    <strong>Average CPU usage:</strong> {avg_cpu:.1f}%<br>
                    <strong>Peak CPU usage:</strong> {max_cpu:.1f}%<br>
                    <strong>Average memory usage:</strong> {avg_memory:.1f} MB<br>
                    <strong>Peak memory usage:</strong> {max_memory:.1f} MB<br>
                </p>
            </div>
            
            <div class="section">
                <h2>System Information</h2>
                <table>
                    <tr><th>Property</th><th>Value</th></tr>
                    <tr><td>OS</td><td>{system_info["os"]}</td></tr>
                    <tr><td>Python Version</td><td>{system_info["python_version"]}</td></tr>
                    <tr><td>CPU Cores</td><td>{system_info["cpu_count"]}</td></tr>
                    <tr><td>Total Memory</td><td>{system_info["total_memory_gb"]:.2f} GB</td></tr>
                    <tr><td>DuckDB Version</td><td>{system_info["duckdb_version"]}</td></tr>
                </table>
            </div>
            
            <div class="section">
                <h2>Resource Usage</h2>
                <div class="chart-container">
                    <img src="resource_usage.png" alt="Resource Usage" style="max-width:100%;">
                </div>
            </div>
            
            <div class="section">
                <h2>Query Performance</h2>
                <div class="chart-container">
                    <img src="query_performance.png" alt="Query Performance" style="max-width:100%;">
                </div>
                
                <h3>Query Details</h3>
                <table>
                    <tr>
                        <th>Query</th>
                        <th>Avg Time (ms)</th>
                        <th>Min Time (ms)</th>
                        <th>Max Time (ms)</th>
                        <th>Executions</th>
                    </tr>
        """
        
        # Add query rows
        for query in query_summaries:
            html_content += f"""
                    <tr>
                        <td>{query["name"]}</td>
                        <td>{query["avg_ms"]:.2f}</td>
                        <td>{query["min_ms"]:.2f}</td>
                        <td>{query["max_ms"]:.2f}</td>
                        <td>{query["count"]}</td>
                    </tr>
            """
        
        # Finish HTML
        html_content += """
                </table>
            </div>
            
            <div class="section">
                <h2>Event Timeline</h2>
                <table>
                    <tr>
                        <th>Time (s)</th>
                        <th>Event</th>
                    </tr>
        """
        
        # Add events
        for event in metrics["events"]:
            html_content += f"""
                    <tr>
                        <td>{event["time"]:.2f}</td>
                        <td>{event["name"]}</td>
                    </tr>
            """
        
        # Finish HTML
        html_content += """
                </table>
            </div>
        </body>
        </html>
        """
        
        # Write HTML file
        with open(os.path.join(report_dir, "report.html"), 'w') as f:
            f.write(html_content)


# Example usage
if __name__ == "__main__":
    monitor = PerformanceMonitor()
    
    # Start background monitoring
    monitor.start_monitoring(interval=0.5)  # Capture metrics every 0.5 seconds
    
    try:
        # Connect to DuckDB
        conn = duckdb.connect(':memory:')
        
        # Record an event
        monitor.record_event("Creating tables")
        
        # Create test table
        conn.execute("""
            CREATE TABLE test AS
            SELECT 
                range % 1000 as id,
                range as value,
                CASE range % 5
                    WHEN 0 THEN 'A'
                    WHEN 1 THEN 'B'
                    WHEN 2 THEN 'C'
                    WHEN 3 THEN 'D'
                    ELSE 'E'
                END as category
            FROM range(1000000)
        """)
        
        # Run test queries and record performance
        monitor.record_query("Simple Count", conn, "SELECT COUNT(*) FROM test")
        monitor.record_query("Filtered Count", conn, "SELECT COUNT(*) FROM test WHERE id < 500")
        monitor.record_query("Group By", conn, "SELECT category, AVG(value) FROM test GROUP BY category")
        monitor.record_query("Order By", conn, "SELECT * FROM test ORDER BY value DESC LIMIT 1000")
        monitor.record_query("Window Function", conn, 
                           """SELECT id, value, category, 
                                 AVG(value) OVER (PARTITION BY category) as avg_by_category
                              FROM test LIMIT 1000""")
        
        # Run a more complex query
        monitor.record_event("Running complex query")
        monitor.record_query("Complex Join", conn, 
                           """WITH t1 AS (
                                  SELECT id, value, category FROM test WHERE id < 500
                              ),
                              t2 AS (
                                  SELECT id, value, category FROM test WHERE id >= 500
                              )
                              SELECT 
                                  t1.category,
                                  COUNT(*) as count,
                                  AVG(t1.value) as avg_t1,
                                  AVG(t2.value) as avg_t2
                              FROM t1
                              JOIN t2 ON t1.id = t2.id - 500
                              GROUP BY t1.category
                              ORDER BY count DESC""")
        
    finally:
        # Stop monitoring and save results
        monitor.stop_monitoring()
        metrics_file = monitor.save_metrics()
        
        # Generate report
        report_dir = monitor.generate_report(metrics_file)
        print(f"Open {report_dir}/report.html to view the performance report")