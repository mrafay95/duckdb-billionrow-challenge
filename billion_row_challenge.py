
# billion_row_challenge.py
# DuckDB Billion Row Challenge for Python with Performance Monitoring

import duckdb
import os
import time
import sys
from datetime import datetime, timedelta  # Make sure to import timedelta
import multiprocessing

# Import the performance monitor (if available)
try:
    sys.path.append('/app/scripts')
    from performance_monitor import PerformanceMonitor
    monitoring_available = True
except ImportError:
    monitoring_available = False
    print("Performance monitoring not available, continuing without it")


# Configuration
DATA_DIR = os.path.join(os.getcwd(), 'data')
PERF_DIR = os.path.join(DATA_DIR, 'performance')
ROW_COUNT = int(os.environ.get('ROW_COUNT', 10000000))  # Default: 10M rows
CITY_COUNT = 1000
# In billion_row_challenge.py
CHUNK_SIZE = 10000000  # 10 million rows per chunk (instead of 1 million)

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PERF_DIR, exist_ok=True)

def print_memory_usage():
    """Print current memory usage"""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        print(f"Memory usage: {memory_info.rss / (1024 * 1024):.2f} MB")
    except ImportError:
        print("psutil not available for memory tracking")

def get_available_memory():
    """Get available memory in bytes"""
    try:
        import psutil
        return psutil.virtual_memory().available
    except ImportError:
        # Default to 8GB if psutil not available
        return 8 * 1024 * 1024 * 1024

# This function fixes the timestamp calculation for each chunk
def generate_parquet_file(conn, chunk_id, row_count, monitor=None):
    """Generate a chunk of data and save to Parquet"""
    chunk_file = os.path.join(DATA_DIR, f"weather_chunk_{chunk_id}.parquet")
    
    # Skip if file already exists
    if os.path.exists(chunk_file):
        if monitor:
            monitor.record_event(f"Chunk {chunk_id} already exists, skipping generation")
        print(f"Chunk {chunk_id} already exists, skipping generation")
        return chunk_file
    
    if monitor:
        monitor.record_event(f"Start generating chunk {chunk_id}")
    print(f"Generating chunk {chunk_id} with {row_count:,} rows")
    
    # Create a table for the chunk
    conn.execute("""
        CREATE TABLE IF NOT EXISTS temp_chunk (
            timestamp TIMESTAMP,
            city_id INTEGER,
            temperature_c DOUBLE,
            humidity_pct INTEGER,
            pressure_hpa DOUBLE,
            wind_speed_kmh DOUBLE,
            weather_condition VARCHAR
        )
    """)
    
    # Calculate base timestamp for this chunk
    # Use date addition instead of hour manipulation to avoid hour overflow
    hours_offset = chunk_id * (row_count // CITY_COUNT)
    days = hours_offset // 24
    remaining_hours = hours_offset % 24
    
    # Format the starting timestamp correctly
    start_date = datetime(2020, 1, 1, remaining_hours, 0, 0)
    if days > 0:
        start_date = start_date + timedelta(days=days)
    
    start_timestamp = start_date.isoformat()
    
    # Generate data directly in DuckDB using SQL
    conn.execute(f"""
        INSERT INTO temp_chunk
        SELECT 
            TIMESTAMP '{start_timestamp}' + INTERVAL ((row_id / {CITY_COUNT})::INTEGER) HOUR as timestamp,
            row_id % {CITY_COUNT} as city_id,
            (random() * 50 - 20)::DOUBLE as temperature_c,
            (random() * 100)::INTEGER as humidity_pct,
            (random() * 50 + 975)::DOUBLE as pressure_hpa,
            (random() * 100)::DOUBLE as wind_speed_kmh,
            CASE (abs(random()) * 7)::INTEGER % 7
                WHEN 0 THEN 'Sunny'
                WHEN 1 THEN 'Cloudy'
                WHEN 2 THEN 'Rainy'
                WHEN 3 THEN 'Snowy'
                WHEN 4 THEN 'Windy'
                WHEN 5 THEN 'Foggy'
                ELSE 'Stormy'
            END as weather_condition
        FROM range(0, {row_count}) t(row_id)
    """)
    
    # Save to Parquet
    if monitor:
        monitor.record_event(f"Writing chunk {chunk_id} to Parquet")
    # When writing to Parquet
    conn.execute(f"COPY temp_chunk TO '{chunk_file}' (FORMAT 'parquet', COMPRESSION 'ZSTD')")
    
    # Drop the temporary table
    conn.execute("DROP TABLE temp_chunk")
    
    if monitor:
        monitor.record_event(f"Finished generating chunk {chunk_id}")
    return chunk_file

def run_billion_row_challenge():
    """Run the 1 billion row challenge with DuckDB"""
    # Initialize performance monitor if available
    monitor = None
    if monitoring_available:
        monitor = PerformanceMonitor(output_dir=PERF_DIR)
        monitor.start_monitoring(interval=1.0)  # Capture metrics every 1 second
        monitor.record_event(f"Starting DuckDB Billion Row Challenge with {ROW_COUNT:,} rows")
    
    start_time_total = time.time()
    
    print(f"Starting DuckDB Billion Row Challenge (Python)")
    print(f"Target row count: {ROW_COUNT:,}")
    
    # Create DuckDB connection
    print("Initializing DuckDB...")
    conn = duckdb.connect(':memory:')
    
    # Configure DuckDB for performance
    available_memory = get_available_memory()
    memory_limit = min(int(available_memory * 0.8), 32 * 1024 * 1024 * 1024)  # Use 80% of available RAM, max 32GB
    print(f"Setting memory limit to {memory_limit / (1024**3):.2f} GB")
    conn.execute(f"SET memory_limit='{memory_limit} B'")
    
    # Set threads to number of CPU cores (safer than using 0)
    cpu_count = multiprocessing.cpu_count()
    print(f"Setting threads to {cpu_count} (number of CPU cores)")
    conn.execute(f"SET threads TO {cpu_count}")
    
    try:
        print_memory_usage()
        
        # Enable profiling if available
        try:
            conn.execute("PRAGMA enable_profiling")
            conn.execute("PRAGMA profiling_mode='detailed'")
            if monitor:
                monitor.record_event("Profiling enabled")
        except:
            print("Note: Profiling not available in this DuckDB version")
            if monitor:
                monitor.record_event("Profiling not available")
        
        # Generate data in chunks
        chunk_count = (ROW_COUNT + CHUNK_SIZE - 1) // CHUNK_SIZE  # Ceiling division
        if monitor:
            monitor.record_event(f"Starting data generation - {chunk_count} chunks")
        print(f"Generating {chunk_count} chunks of data...")
        
        chunk_files = []
        for i in range(chunk_count):
            rows_in_chunk = min(CHUNK_SIZE, ROW_COUNT - i * CHUNK_SIZE)
            chunk_file = generate_parquet_file(conn, i, rows_in_chunk, monitor)
            chunk_files.append(chunk_file)
            print(f"Progress: {round((i + 1) / chunk_count * 100)}%")
        
        # Create the final table
        if monitor:
            monitor.record_event("Creating weather table")
        print('Creating weather table...')
        conn.execute("""
            CREATE TABLE weather (
                timestamp TIMESTAMP,
                city_id INTEGER,
                temperature_c DOUBLE,
                humidity_pct INTEGER,
                pressure_hpa DOUBLE,
                wind_speed_kmh DOUBLE,
                weather_condition VARCHAR
            )
        """)
        
        # Load data from Parquet files
        if monitor:
            monitor.record_event("Loading data from Parquet files")
        print('Loading data from Parquet files...')
        load_start = time.time()
        
        for i, parquet_file in enumerate(chunk_files):
            if monitor:
                monitor.record_event(f"Loading file {i+1}/{len(chunk_files)}")
            print(f"Loading file {i+1}/{len(chunk_files)}")
            conn.execute(f"""
                INSERT INTO weather 
                SELECT * FROM read_parquet('{parquet_file}')
            """)
        
        load_time = time.time() - load_start
        if monitor:
            monitor.record_event(f"Data loading complete in {load_time:.2f} seconds")
        print(f"Data loading complete in {load_time:.2f} seconds")
        
        # Verify row count
        count_result = conn.execute("SELECT COUNT(*) FROM weather").fetchone()
        actual_rows = count_result[0]
        if monitor:
            monitor.record_event(f"Loaded {actual_rows:,} rows")
        print(f"Loaded {actual_rows:,} rows")
        
        print_memory_usage()
        
        # Run challenge queries
        if monitor:
            monitor.record_event("Starting analytics queries")
        print("\n--- Running analytics queries ---")
        
        # 1. Average temperature by city
        print("\nQuery 1: Average temperature by city")
        query1 = """
            SELECT 
                city_id,
                AVG(temperature_c) AS avg_temperature
            FROM weather
            GROUP BY city_id
            ORDER BY avg_temperature DESC
            LIMIT 10
        """
        
        # Execute the query with or without monitoring
        q1_start = time.time()
        if monitor:
            result1, q1_time = monitor.record_query("Average temperature by city", conn, query1)
            print(f"Query 1 completed in {q1_time:.3f} ms")
        else:
            result1 = conn.execute(query1).fetchall()
            q1_time = (time.time() - q1_start) * 1000
            print(f"Query 1 completed in {q1_time:.3f} ms")
            
        print("Top 10 cities by average temperature:")
        for row in result1:
            print(f"City {row[0]}: {row[1]:.2f}°C")
        
        # 2. Monthly statistics by city
        print("\nQuery 2: Monthly statistics by city")
        query2 = """
            SELECT 
                city_id,
                DATE_TRUNC('month', timestamp) AS month,
                MIN(temperature_c) AS min_temp,
                MAX(temperature_c) AS max_temp,
                AVG(temperature_c) AS avg_temp,
                MIN(humidity_pct) AS min_humidity,
                MAX(humidity_pct) AS max_humidity,
                AVG(humidity_pct) AS avg_humidity
            FROM weather
            WHERE city_id < 5
            GROUP BY city_id, month
            ORDER BY city_id, month
            LIMIT 10
        """
        
        q2_start = time.time()
        if monitor:
            result2, q2_time = monitor.record_query("Monthly statistics by city", conn, query2)
            print(f"Query 2 completed in {q2_time:.3f} ms")
        else:
            result2 = conn.execute(query2).fetchall()
            q2_time = (time.time() - q2_start) * 1000
            print(f"Query 2 completed in {q2_time:.3f} ms")
            
        print("Sample monthly statistics:")
        for row in result2:
            print(f"City {row[0]}, {row[1]}: Min: {row[2]:.1f}°C, Max: {row[3]:.1f}°C, Avg: {row[4]:.1f}°C")
        
        # 3. Find extreme weather events
        print("\nQuery 3: Extreme weather events")
        query3 = """
            SELECT 
                timestamp,
                city_id,
                temperature_c,
                humidity_pct,
                wind_speed_kmh,
                weather_condition
            FROM weather
            WHERE 
                (temperature_c < -15 OR temperature_c > 40) OR
                (wind_speed_kmh > 80) OR
                (humidity_pct > 95 AND temperature_c > 30)
            ORDER BY temperature_c DESC
            LIMIT 10
        """
        
        q3_start = time.time()
        if monitor:
            result3, q3_time = monitor.record_query("Extreme weather events", conn, query3)
            print(f"Query 3 completed in {q3_time:.3f} ms")
        else:
            result3 = conn.execute(query3).fetchall()
            q3_time = (time.time() - q3_start) * 1000
            print(f"Query 3 completed in {q3_time:.3f} ms")
            
        print("Top 10 extreme weather events:")
        for row in result3:
            print(f"City {row[1]} at {row[0]}: {row[2]:.1f}°C, {row[3]}% humidity, {row[4]:.1f} km/h wind ({row[5]})")
        
        # 4. Complex window functions
        print("\nQuery 4: Moving averages with window functions")
        query4 = """
            WITH daily_avg AS (
                SELECT 
                    city_id,
                    DATE_TRUNC('day', timestamp) AS day,
                    AVG(temperature_c) AS daily_avg_temp
                FROM weather
                GROUP BY city_id, day
            )
            SELECT 
                city_id,
                day,
                daily_avg_temp,
                AVG(daily_avg_temp) OVER (
                    PARTITION BY city_id 
                    ORDER BY day 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS seven_day_moving_avg
            FROM daily_avg
            WHERE city_id < 5
            ORDER BY city_id, day
            LIMIT 20
        """
        
        q4_start = time.time()
        if monitor:
            result4, q4_time = monitor.record_query("Moving averages", conn, query4)
            print(f"Query 4 completed in {q4_time:.3f} ms")
        else:
            result4 = conn.execute(query4).fetchall()
            q4_time = (time.time() - q4_start) * 1000
            print(f"Query 4 completed in {q4_time:.3f} ms")
            
        print("Sample of 7-day moving averages:")
        for row in result4:
            print(f"City {row[0]}, {row[1]}: Daily: {row[2]:.1f}°C, 7-day avg: {row[3]:.1f}°C")
            
        # 5. Correlation analysis
        print("\nQuery 5: Weather correlation analysis")
        query5 = """
            SELECT 
                city_id,
                CORR(temperature_c, humidity_pct) AS temp_humidity_corr,
                CORR(temperature_c, wind_speed_kmh) AS temp_wind_corr,
                CORR(humidity_pct, pressure_hpa) AS humidity_pressure_corr
            FROM weather
            GROUP BY city_id
            ORDER BY temp_humidity_corr DESC
            LIMIT 10
        """
        
        q5_start = time.time()
        if monitor:
            result5, q5_time = monitor.record_query("Correlation analysis", conn, query5)
            print(f"Query 5 completed in {q5_time:.3f} ms")
        else:
            result5 = conn.execute(query5).fetchall()
            q5_time = (time.time() - q5_start) * 1000
            print(f"Query 5 completed in {q5_time:.3f} ms")
            
        print("Temperature-humidity correlation for top 10 cities:")
        for row in result5:
            print(f"City {row[0]}: Temp-Humidity: {row[1]:.3f}, Temp-Wind: {row[2]:.3f}, Humidity-Pressure: {row[3]:.3f}")
            
        # Try to get query execution plans
        try:
            if monitor:
                monitor.record_event("Generating query plans")
            print("\nQuery execution plan for Query 1:")
            plan = conn.execute("EXPLAIN " + query1).fetchall()
            for line in plan:
                print(line[0])
        except:
            print("Note: EXPLAIN not available in this DuckDB version")
            
        # Try to get profiling information
        try:
            if monitor:
                monitor.record_event("Getting profiling information")
            print("\nProfiling information:")
            profiling = conn.execute("PRAGMA print_profiling").fetchall()
            print(f"Number of profiling entries: {len(profiling)}")
        except:
            print("Note: Profiling output not available")
        
        # Summary
        total_time = time.time() - start_time_total
        if monitor:
            monitor.record_event(f"Challenge complete in {total_time:.2f} seconds")
        print("\n--- 1 Billion Row Challenge Summary ---")
        print(f"Total rows processed: {actual_rows:,}")
        print(f"Total execution time: {total_time:.2f} seconds")
        
        query_times = [q1_time, q2_time, q3_time, q4_time, q5_time]
        print(f"Average query time: {sum(query_times)/len(query_times):.3f} ms")
        print(f"Fastest query: {min(query_times):.3f} ms")
        print(f"Slowest query: {max(query_times):.3f} ms")
        
    except Exception as e:
        if monitor:
            monitor.record_event(f"Error: {str(e)}")
        print(f"Error in billion row challenge: {e}")
    finally:
        # Stop monitoring and generate report if available
        if monitor:
            try:
                monitor.record_event("Generating performance report")
                monitor.stop_monitoring()
                metrics_file = monitor.save_metrics()
                report_dir = monitor.generate_report(metrics_file)
                print(f"Performance report saved to {report_dir}/report.html")
                print("Access this report in the 'data/performance/report' directory on your host machine")
            except Exception as e:
                print(f"Error generating performance report: {e}")
        
        print_memory_usage()
        conn.close()

if __name__ == "__main__":
    run_billion_row_challenge()