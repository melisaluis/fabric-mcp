"""
Query suggestion service - helps new users get started with tables.

Generates context-aware starter queries based on table structure.
"""
from typing import List, Dict, Any
from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_sql_query


def suggest_queries_for_table(schema_name: str, table_name: str) -> List[Dict[str, str]]:
    """
    Generate helpful starter queries for a table to guide new users.
    
    Creates queries for common exploration patterns:
    - Preview first rows
    - Count records
    - Find recent data (if date columns exist)
    - Check value distributions in key columns
    - Identify duplicates in ID columns
    
    Args:
        schema_name: Schema name
        table_name: Table name
    
    Returns:
        List of suggested queries with descriptions and executable SQL
        
    Example output:
        [
            {
                'description': 'Preview first 10 rows to see sample data',
                'query': 'SELECT TOP 10 * FROM [ICM].[Incidents]',
                'purpose': 'data_preview'
            },
            ...
        ]
    """
    full_table_name = f"[{schema_name}].[{table_name}]"
    
    # Get column info to generate smart queries
    column_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """
    columns = lakehouse_sql_query(column_query)
    
    suggestions = []
    
    # 1. Basic preview
    suggestions.append({
        'description': '👀 Preview first 10 rows to see sample data',
        'query': f"SELECT TOP 10 * FROM {full_table_name}",
        'purpose': 'data_preview'
    })
    
    # 2. Row count
    suggestions.append({
        'description': '📊 Count total number of records',
        'query': f"SELECT COUNT(*) as total_records FROM {full_table_name}",
        'purpose': 'size_check'
    })
    
    # 3. Find date/time columns for freshness check
    date_columns = [col[0] for col in columns if 'date' in col[1].lower() or 'time' in col[1].lower()]
    if date_columns:
        date_col = date_columns[0]
        suggestions.append({
            'description': f'📅 Check data freshness - when was data last updated?',
            'query': f"""SELECT 
    MIN([{date_col}]) as oldest_record,
    MAX([{date_col}]) as newest_record,
    COUNT(*) as total_records,
    DATEDIFF(day, MIN([{date_col}]), MAX([{date_col}])) as days_span
FROM {full_table_name}""",
            'purpose': 'data_freshness'
        })
        
        suggestions.append({
            'description': f'🕐 Show most recent 10 records',
            'query': f"SELECT TOP 10 * FROM {full_table_name} ORDER BY [{date_col}] DESC",
            'purpose': 'recent_data'
        })
    
    # 4. Find string columns for value distribution
    string_columns = [col[0] for col in columns if 'varchar' in col[1].lower() or 'char' in col[1].lower()]
    if string_columns:
        # Pick the most interesting column (likely status, type, category fields)
        interesting = None
        for col in string_columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['status', 'type', 'category', 'severity', 'priority', 'state']):
                interesting = col
                break
        
        if not interesting and string_columns:
            interesting = string_columns[0]
        
        if interesting:
            suggestions.append({
                'description': f'📈 Show distribution of values in [{interesting}]',
                'query': f"""SELECT 
    [{interesting}],
    COUNT(*) as count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) as percentage
FROM {full_table_name}
GROUP BY [{interesting}]
ORDER BY COUNT(*) DESC""",
                'purpose': 'value_distribution'
            })
    
    # 5. Find potential ID/key columns and check uniqueness
    id_columns = [col[0] for col in columns if 'id' in col[0].lower() and not 'guid' in col[0].lower()]
    if id_columns:
        id_col = id_columns[0]
        suggestions.append({
            'description': f'🔑 Check if [{id_col}] is unique (find duplicates)',
            'query': f"""SELECT 
    [{id_col}],
    COUNT(*) as frequency
FROM {full_table_name}
GROUP BY [{id_col}]
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC""",
            'purpose': 'uniqueness_check'
        })
    
    # 6. Null analysis - find columns with missing data
    suggestions.append({
        'description': '❓ Check for null/missing values across columns',
        'query': f"""SELECT {chr(10).join([f"    SUM(CASE WHEN [{col[0]}] IS NULL THEN 1 ELSE 0 END) as [{col[0]}_nulls]," for col in columns[:5]])}
    COUNT(*) as total_rows
FROM {full_table_name}""",
        'purpose': 'data_quality'
    })
    
    # 7. If there are numeric columns, show basic stats
    numeric_columns = [col[0] for col in columns if col[1].lower() in ('int', 'bigint', 'decimal', 'numeric', 'float', 'real')]
    if numeric_columns:
        num_col = numeric_columns[0]
        suggestions.append({
            'description': f'📉 Basic statistics for numeric column [{num_col}]',
            'query': f"""SELECT 
    MIN([{num_col}]) as min_value,
    MAX([{num_col}]) as max_value,
    AVG(CAST([{num_col}] AS FLOAT)) as avg_value,
    COUNT(DISTINCT [{num_col}]) as distinct_values
FROM {full_table_name}
WHERE [{num_col}] IS NOT NULL""",
            'purpose': 'numeric_analysis'
        })
    
    return suggestions


def get_exploration_guide(schema_name: str) -> Dict[str, Any]:
    """
    Alias for get_exploration_path. Generates a guided exploration path for a schema.
    """
    return get_exploration_path(schema_name)

def get_exploration_path(schema_name: str) -> Dict[str, Any]:
    """
    Generate a guided exploration path for a schema.
    
    Suggests which tables to look at first and in what order.
    
    Args:
        schema_name: Schema name
    
    Returns:
        Dictionary with exploration recommendations
    """
    # Get tables in schema with row counts
    tables_query = f"""
        SELECT 
            t.TABLE_NAME,
            SUM(p.rows) as row_count
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN sys.objects o ON o.name = t.TABLE_NAME AND o.type = 'U'
        LEFT JOIN sys.partitions p ON p.object_id = o.object_id AND p.index_id IN (0,1)
        WHERE t.TABLE_SCHEMA = '{schema_name}' AND t.TABLE_TYPE = 'BASE TABLE'
        GROUP BY t.TABLE_NAME
        ORDER BY SUM(p.rows) DESC
    """
    
    tables = lakehouse_sql_query(tables_query)
    
    if not tables:
        return {
            'schema': schema_name,
            'recommendation': f"No tables found in schema '{schema_name}'"
        }
    
    # Categorize tables
    small_tables = [t for t in tables if (t[1] or 0) < 1000]
    large_tables = [t for t in tables if (t[1] or 0) >= 10000]
    medium_tables = [t for t in tables if t not in small_tables and t not in large_tables]
    
    return {
        'schema': schema_name,
        'total_tables': len(tables),
        'exploration_steps': [
            {
                'step': 1,
                'action': 'Start with small reference tables',
                'tables': [t[0] for t in small_tables[:3]],
                'reason': 'These are likely lookup/reference tables - understanding them first helps interpret the main data'
            },
            {
                'step': 2,
                'action': 'Explore medium-sized fact tables',
                'tables': [t[0] for t in medium_tables[:3]],
                'reason': 'These contain the core business data and are easier to work with than the largest tables'
            },
            {
                'step': 3,
                'action': 'Investigate large tables (be cautious with queries)',
                'tables': [t[0] for t in large_tables[:3]],
                'reason': 'These have the most data - use TOP/WHERE clauses to avoid slow queries'
            }
        ],
        'quick_start_query': f"-- Get an overview of all tables in {schema_name}\n" +
                            f"SELECT TABLE_NAME, \n" +
                            f"       (SELECT COUNT(*) FROM [{schema_name}].[' + TABLE_NAME + ']) as row_count\n" +
                            f"FROM INFORMATION_SCHEMA.TABLES\n" +
                            f"WHERE TABLE_SCHEMA = '{schema_name}'\n" +
                            f"ORDER BY TABLE_NAME"
    }
