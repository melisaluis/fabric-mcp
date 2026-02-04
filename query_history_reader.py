"""
Query History Reader - Search and analyze logged query history.

Reads the query_history_log.jsonl file created by query_history_logger.py
"""
import json
from datetime import datetime, timedelta
from collections import defaultdict

LOG_FILE = "query_history_log.jsonl"

def read_query_history(hours_back=24, search_text=None, min_cpu_ms=None):
    """
    Read query history from log file with filters.
    
    Args:
        hours_back: How many hours back to search (default 24)
        search_text: Filter queries containing this text (case-insensitive)
        min_cpu_ms: Only show queries with CPU time >= this threshold
    """
    cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
    queries = []
    
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                entry = json.loads(line)
                
                # Parse captured timestamp
                captured_at = datetime.fromisoformat(entry["captured_at"])
                if captured_at < cutoff_time:
                    continue
                
                # Apply filters
                if search_text and search_text.lower() not in entry["query_text"].lower():
                    continue
                
                if min_cpu_ms and entry["cpu_time_ms"] < min_cpu_ms:
                    continue
                
                queries.append(entry)
        
        return queries
    except FileNotFoundError:
        print(f"Log file '{LOG_FILE}' not found. Run query_history_logger.py first.")
        return []

def summarize_queries(queries):
    """Generate summary statistics from query history."""
    if not queries:
        print("No queries found.")
        return
    
    # Group by query text (first 100 chars for similarity)
    query_groups = defaultdict(list)
    for q in queries:
        key = q["query_text"][:100]
        query_groups[key].append(q)
    
    print(f"\nTotal query executions logged: {len(queries)}")
    print(f"Unique query patterns: {len(query_groups)}")
    print(f"\nTop 10 Most Frequent Queries:")
    print("-" * 70)
    
    # Sort by frequency
    sorted_groups = sorted(query_groups.items(), key=lambda x: len(x[1]), reverse=True)
    
    for i, (query_pattern, executions) in enumerate(sorted_groups[:10], 1):
        total_cpu = sum(e["cpu_time_ms"] for e in executions)
        avg_cpu = total_cpu / len(executions)
        print(f"{i}. {query_pattern}...")
        print(f"   Executions: {len(executions)} | Avg CPU: {avg_cpu:.0f}ms | Total CPU: {total_cpu:.0f}ms")
        print()

def main():
    print("=" * 60)
    print("Query History Reader")
    print("=" * 60)
    
    # Example: Read last 24 hours
    print("\nReading query history from last 24 hours...")
    queries = read_query_history(hours_back=24)
    summarize_queries(queries)
    
    # Example: Find expensive queries
    print("\n" + "=" * 60)
    print("Expensive Queries (>100ms CPU)")
    print("=" * 60)
    expensive = read_query_history(hours_back=24, min_cpu_ms=100)
    
    for i, q in enumerate(expensive[:10], 1):
        print(f"{i}. CPU: {q['cpu_time_ms']:.0f}ms | Elapsed: {q['elapsed_time_ms']:.0f}ms")
        print(f"   Last Run: {q['last_execution_time']}")
        print(f"   Query: {q['query_text'][:100]}...")
        print()

if __name__ == "__main__":
    main()
