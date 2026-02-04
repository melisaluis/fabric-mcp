"""
Query History Logger - Periodically captures and stores query history.

This runs in the background and saves query history to avoid cache eviction issues.
Run this continuously to build up historical data.
"""
import os
import time
import json
from datetime import datetime
from pathlib import Path
from fabric_rti_mcp.tools.query_history_tool import get_query_history

# Configuration
CAPTURE_INTERVAL_SECONDS = 300  # Capture every 5 minutes
LOG_FILE = "query_history_log.jsonl"  # JSON Lines format

os.environ["FABRIC_SQL_ENDPOINT"] = "x6eps4xrq2xudenlfv6naeo3i4-k7qssngppcxuhaxhdc2h6hdjgq.msit-datawarehouse.fabric.microsoft.com"
os.environ["FABRIC_LAKEHOUSE_NAME"] = "Starbase"

def capture_and_log():
    """Capture current query history and append to log file."""
    try:
        queries = get_query_history(top_n=100)
        
        timestamp = datetime.utcnow().isoformat()
        for query in queries:
            # Convert to dict for JSON serialization
            entry = {
                "captured_at": timestamp,
                "execution_count": query[0],
                "creation_time": str(query[1]),
                "last_execution_time": str(query[2]),
                "cpu_time_ms": query[3],
                "elapsed_time_ms": query[4],
                "logical_reads": query[5],
                "logical_writes": query[6],
                "query_text": str(query[7])[:500]  # Truncate long queries
            }
            
            # Append to JSONL file (one JSON object per line)
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        
        print(f"[{datetime.now()}] Captured {len(queries)} queries")
        
    except Exception as e:
        print(f"[{datetime.now()}] Error: {e}")

def main():
    print("=" * 60)
    print("Query History Logger - Running")
    print("=" * 60)
    print(f"Logging to: {LOG_FILE}")
    print(f"Capture interval: {CAPTURE_INTERVAL_SECONDS} seconds")
    print("\nPress Ctrl+C to stop\n")
    
    # Create log file if doesn't exist
    Path(LOG_FILE).touch()
    
    try:
        while True:
            capture_and_log()
            time.sleep(CAPTURE_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n\nStopping logger...")

if __name__ == "__main__":
    main()
