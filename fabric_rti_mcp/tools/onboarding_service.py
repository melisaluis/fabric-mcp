"""
Onboarding service for helping new users familiarize themselves with the database.

Provides data profiling, smart suggestions, and guided exploration.
"""
from typing import Dict, List, Tuple, Optional, Any
from collections import Counter
from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_sql_query


def profile_table(schema_name: str, table_name: str, sample_size: int = 1000) -> Dict[str, Any]:
    """
    Generate a comprehensive profile of a table to help users understand its contents.
    
    Returns statistics including:
    - Row count and data freshness
    - Column information with data types and sample values
    - Null percentages
    - Unique value counts for categorical columns
    - Basic statistics for numeric columns
    
    Args:
        schema_name: Schema name
        table_name: Table name
        sample_size: Number of rows to sample for detailed analysis (default 1000)
    
    Returns:
        Dictionary containing profile information
    """
    full_table_name = f"[{schema_name}].[{table_name}]"
    
    # Get basic table info
    row_count_query = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
    row_count = execute_sql_query(row_count_query)[0][0]
    row_count = int(row_count) if row_count is not None else 0
    
    # Get column information
    column_info_query = f"""
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = '{schema_name}' AND c.TABLE_NAME = '{table_name}'
        ORDER BY c.ORDINAL_POSITION
    """
    columns = execute_sql_query(column_info_query)
    
    column_profiles = []
    for col in columns:
        col_name = col[0]
        data_type = col[1]
        is_nullable = col[5]
        
        # Get null percentage
        null_query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN [{col_name}] IS NULL THEN 1 ELSE 0 END) as null_count
            FROM {full_table_name}
        """
        total, null_count = execute_sql_query(null_query)[0]
        null_pct = (null_count / total * 100) if total > 0 else 0
        
        # Get distinct count (for smaller tables or sampled data)
        if row_count <= 10000:
            distinct_query = f"SELECT COUNT(DISTINCT [{col_name}]) FROM {full_table_name}"
        else:
            distinct_query = f"SELECT COUNT(DISTINCT [{col_name}]) FROM (SELECT TOP {sample_size} [{col_name}] FROM {full_table_name}) AS sample"
        
        try:
            distinct_count = execute_sql_query(distinct_query)[0][0]
        except:
            distinct_count = None
        
        # Get sample values (top 5 most common)
        sample_values = []
        if data_type in ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext'):
            try:
                sample_query = f"""
                    SELECT TOP 5 [{col_name}], COUNT(*) as freq
                    FROM {full_table_name}
                    WHERE [{col_name}] IS NOT NULL
                    GROUP BY [{col_name}]
                    ORDER BY COUNT(*) DESC
                """
                sample_values = execute_sql_query(sample_query)
            except:
                pass
        
        # For numeric columns, get basic stats
        stats = None
        if data_type in ('int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money'):
            try:
                stats_query = f"""
                    SELECT 
                        MIN([{col_name}]) as min_val,
                        MAX([{col_name}]) as max_val,
                        AVG(CAST([{col_name}] AS FLOAT)) as avg_val
                    FROM {full_table_name}
                    WHERE [{col_name}] IS NOT NULL
                """
                stats_result = execute_sql_query(stats_query)[0]
                stats = {
                    'min': stats_result[0],
                    'max': stats_result[1],
                    'avg': round(stats_result[2], 2) if stats_result[2] else None
                }
            except:
                pass
        
        column_profiles.append({
            'name': col_name,
            'data_type': data_type,
            'is_nullable': is_nullable == 'YES',
            'null_percentage': round(null_pct, 2),
            'distinct_count': distinct_count,
            'sample_values': sample_values[:5] if sample_values else [],
            'statistics': stats
        })
    
    return {
        'schema': schema_name,
        'table': table_name,
        'row_count': row_count,
        'column_count': len(column_profiles),
        'columns': column_profiles
    }


def get_schema_overview(schema_name: str) -> Dict[str, Any]:
    """
    Get a high-level overview of a schema to help users understand its purpose.
    
    Includes table list, relationships, and overall statistics.
    
    Args:
        schema_name: Schema name
    
    Returns:
        Dictionary with schema overview
    """
    # Get all tables in schema with row counts
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
    tables = execute_sql_query(tables_query)
    
    # Get foreign key relationships within this schema
    relationships_query = f"""
        SELECT 
            fk.name as constraint_name,
            tp.name as parent_table,
            cp.name as parent_column,
            tr.name as referenced_table,
            cr.name as referenced_column
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
        INNER JOIN sys.schemas sp ON tp.schema_id = sp.schema_id
        INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
        INNER JOIN sys.schemas sr ON tr.schema_id = sr.schema_id
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        WHERE sp.name = '{schema_name}' OR sr.name = '{schema_name}'
    """
    
    try:
        relationships = execute_sql_query(relationships_query)
    except:
        relationships = []
    
    total_rows = sum(int(t[1]) if t[1] is not None else 0 for t in tables)
    
    return {
        'schema': schema_name,
        'table_count': len(tables),
        'total_rows': total_rows,
        'tables': [{'name': t[0], 'row_count': int(t[1]) if t[1] is not None else 0} for t in tables],
        'relationships': [
            {
                'from_table': r[1],
                'from_column': r[2],
                'to_table': r[3],
                'to_column': r[4]
            } for r in relationships
        ]
    }


def suggest_starter_queries(schema_name: str, table_name: str) -> List[Dict[str, str]]:
    """
    Generate helpful starter queries for a table to guide new users.
    
    Creates queries for common exploration patterns:
    - Preview first rows
    - Count records
    - Check for nulls
    - Find unique values in key columns
    - Look for recent data
    
    Args:
        schema_name: Schema name
        table_name: Table name
    
    Returns:
        List of suggested queries with descriptions
    """
    full_table_name = f"[{schema_name}].[{table_name}]"
    
    # Get column info to generate smart queries
    column_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """
    columns = execute_sql_query(column_query)
    
    suggestions = []
    
    # Basic preview
    suggestions.append({
        'description': 'Preview first 10 rows to see sample data',
        'query': f"SELECT TOP 10 * FROM {full_table_name}"
    })
    
    # Row count
    suggestions.append({
        'description': 'Count total number of records',
        'query': f"SELECT COUNT(*) as total_records FROM {full_table_name}"
    })
    
    # Find date/time columns for freshness check
    date_columns = [col[0] for col in columns if 'date' in col[1].lower() or 'time' in col[1].lower()]
    if date_columns:
        date_col = date_columns[0]
        suggestions.append({
            'description': f'Check data freshness using {date_col}',
            'query': f"SELECT MIN([{date_col}]) as oldest, MAX([{date_col}]) as newest, COUNT(*) as records FROM {full_table_name}"
        })
        
        suggestions.append({
            'description': f'Show most recent 10 records by {date_col}',
            'query': f"SELECT TOP 10 * FROM {full_table_name} ORDER BY [{date_col}] DESC"
        })
    
    # Find potential ID/key columns
    id_columns = [col[0] for col in columns if 'id' in col[0].lower() or 'key' in col[0].lower()]
    if id_columns and len(id_columns) <= 3:
        id_cols = ', '.join([f'[{col}]' for col in id_columns[:2]])
        suggestions.append({
            'description': f'Check uniqueness of key columns',
            'query': f"SELECT {id_cols}, COUNT(*) as frequency FROM {full_table_name} GROUP BY {id_cols} HAVING COUNT(*) > 1"
        })
    
    # Find string columns for value distribution
    string_columns = [col[0] for col in columns if 'varchar' in col[1].lower() or 'char' in col[1].lower()]
    if string_columns:
        str_col = string_columns[0]
        suggestions.append({
            'description': f'Show distribution of values in {str_col}',
            'query': f"SELECT TOP 10 [{str_col}], COUNT(*) as count FROM {full_table_name} GROUP BY [{str_col}] ORDER BY COUNT(*) DESC"
        })
    
    # Null check for nullable columns
    suggestions.append({
        'description': 'Check for null values across all columns',
        'query': f"""
SELECT {', '.join([f"SUM(CASE WHEN [{col[0]}] IS NULL THEN 1 ELSE 0 END) as [{col[0]}_nulls]" for col in columns[:5]])}
FROM {full_table_name}
        """.strip()
    })
    
    return suggestions


def find_related_tables(schema_name: str, table_name: str) -> List[Dict[str, Any]]:
    """
    Find tables related to the given table through foreign keys or naming patterns.
    
    Helps users discover connected data they might want to join.
    
    Args:
        schema_name: Schema name
        table_name: Table name
    
    Returns:
        List of related tables with relationship details
    """
    related = []
    
    # Find direct foreign key relationships
    fk_query = f"""
        SELECT 
            'outgoing' as direction,
            tp.name as from_table,
            SCHEMA_NAME(tp.schema_id) as from_schema,
            cp.name as from_column,
            tr.name as to_table,
            SCHEMA_NAME(tr.schema_id) as to_schema,
            cr.name as to_column
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
        INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        WHERE tp.name = '{table_name}' AND SCHEMA_NAME(tp.schema_id) = '{schema_name}'
        
        UNION ALL
        
        SELECT 
            'incoming' as direction,
            tp.name as from_table,
            SCHEMA_NAME(tp.schema_id) as from_schema,
            cp.name as from_column,
            tr.name as to_table,
            SCHEMA_NAME(tr.schema_id) as to_schema,
            cr.name as to_column
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
        INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        WHERE tr.name = '{table_name}' AND SCHEMA_NAME(tr.schema_id) = '{schema_name}'
    """
    
    try:
        fk_relationships = execute_sql_query(fk_query)
        for rel in fk_relationships:
            related.append({
                'relationship_type': 'foreign_key',
                'direction': rel[0],
                'related_table': f"{rel[5]}.{rel[4]}",
                'join_condition': f"{rel[2]}.{rel[1]}.{rel[3]} = {rel[5]}.{rel[4]}.{rel[6]}",
                'confidence': 'high'
            })
    except:
        pass
    
    # Find potential relationships by column naming patterns
    column_query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        AND (COLUMN_NAME LIKE '%Id' OR COLUMN_NAME LIKE '%Key' OR COLUMN_NAME LIKE '%ID')
    """
    
    try:
        key_columns = execute_sql_query(column_query)
        for col in key_columns:
            col_name = col[0]
            # Look for tables with matching column names
            matching_query = f"""
                SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE COLUMN_NAME = '{col_name}'
                AND NOT (TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}')
            """
            matches = execute_sql_query(matching_query)
            for match in matches:
                related.append({
                    'relationship_type': 'potential_match',
                    'direction': 'unknown',
                    'related_table': f"{match[0]}.{match[1]}",
                    'join_condition': f"Both have column [{col_name}]",
                    'confidence': 'medium'
                })
    except:
        pass
    
    return related


def generate_database_guide() -> Dict[str, Any]:
    """
    Generate a comprehensive onboarding guide for the entire database.
    
    Provides an overview of all schemas, key tables, and suggested exploration paths.
    
    Returns:
        Database guide with schemas, important tables, and getting started tips
    """
    # Get all schemas and their table counts
    schemas_query = """
        SELECT 
            s.name as schema_name,
            COUNT(t.name) as table_count,
            SUM(p.rows) as total_rows
        FROM sys.schemas s
        LEFT JOIN sys.tables t ON t.schema_id = s.schema_id
        LEFT JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1)
        WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'db_owner', 'db_accessadmin', 
                             'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 
                             'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
        GROUP BY s.name
        HAVING COUNT(t.name) > 0
        ORDER BY SUM(p.rows) DESC
    """
    
    from fabric_rti_mcp.tools.lakehouse_sql_tool import lakehouse_sql_query
    schemas = lakehouse_sql_query(schemas_query)
    
    schema_info = []
    for schema in schemas:
        # Get top 5 largest tables in each schema
        top_tables_query = f"""
            SELECT TOP 5
                t.name as table_name,
                SUM(p.rows) as row_count
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1)
            WHERE s.name = '{schema[0]}'
            GROUP BY t.name
            ORDER BY SUM(p.rows) DESC
        """
        top_tables = lakehouse_sql_query(top_tables_query)
        
        schema_info.append({
            'name': schema[0],
            'table_count': int(schema[1]) if schema[1] is not None else 0,
            'total_rows': int(schema[2]) if schema[2] is not None else 0,
            'top_tables': [{'name': t[0], 'row_count': int(t[1]) if t[1] is not None else 0} for t in top_tables]
        })
    
    return {
        'database_name': 'Starbase (Fabric Lakehouse)',
        'total_schemas': len(schema_info),
        'schemas': schema_info,
        'getting_started_tips': [
            "Start by exploring the schema with the most data (usually the main business schema)",
            "Use profile_table() to understand a table's structure and contents",
            "Check suggested queries with suggest_starter_queries() for common patterns",
            "Use find_related_tables() to discover how tables connect",
            "Look for tables with 'Date' or 'Time' columns to understand data freshness"
        ]
    }
