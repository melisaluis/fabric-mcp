"""Generate dataset documentation by calling MCP SQL lakehouse tools.

This script is an example that demonstrates how an orchestrator or analyst notebook can
call the MCP server to discover tables and fetch schema information, then write markdown
documentation files per table.

Usage (PowerShell):

    $env:FABRIC_RTI_MCP_TOKEN = '<token-if-required>'
    python ./examples/generate_dataset_docs.py --host 127.0.0.1 --port 3000 --database MyDatabase --top-n 20

The script expects these MCP tools to be registered on your MCP server:
- `sql_list_lakehouse_tables` (params: {"database": "..."})
- `sql_get_table_schema` (params: {"database": "...", "table": "...", "sample_size": 5})

This is example code: adapt error handling, payload shapes, and the MCP HTTP envelope to match
your MCP HTTP adapter if necessary.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Dict, List

from examples.mcp_client import default_client_from_env, MCPClient


def write_table_md(out_dir: Path, database: str, table: str, schema: Dict[str, Any], sample_rows: List[Dict[str, Any]]) -> None:
    db_dir = out_dir / database
    db_dir.mkdir(parents=True, exist_ok=True)
    file_path = db_dir / f"{table}.md"
    with file_path.open("w", encoding="utf-8") as fh:
        fh.write(f"# Table: {table}\n\n")
        fh.write("## Schema\n\n")
        fh.write("| Column | Type | Nullable |\n")
        fh.write("|---|---|---:|\n")
        for col in schema.get("columns", []):
            name = col.get("name")
            dtype = col.get("type")
            nullable = col.get("nullable")
            fh.write(f"| {name} | {dtype} | {nullable} |\n")

        fh.write("\n## Sample rows\n\n")
        if not sample_rows:
            fh.write("No sample rows returned.\n")
        else:
            # write a simple CSV-like block
            headers = list(sample_rows[0].keys())
            fh.write("| " + " | ".join(headers) + " |\n")
            fh.write("|" + "---|" * len(headers) + "\n")
            for row in sample_rows:
                fh.write("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |\n")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate dataset docs by calling MCP SQL lakehouse tools")
    parser.add_argument("--host", default=os.environ.get("FABRIC_RTI_HTTP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("FABRIC_RTI_HTTP_PORT", "3000")))
    parser.add_argument("--database", required=True)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--out-dir", default="examples/datasets")
    args = parser.parse_args(argv)

    client: MCPClient = default_client_from_env(host=args.host, port=args.port)
    out_dir = Path(args.out_dir)

    print(f"Listing tables in database {args.database} via MCP at {args.host}:{args.port}")
    resp = client.invoke_tool("lakehouse_list_tables_safe", {"database": args.database})
    # Expect response to be something like {"tables": [{"name":"..."}, ...]}
    tables = resp.get("tables") or resp.get("result") or []

    # normalize tables to strings
    table_names = []
    for t in tables:
        if isinstance(t, str):
            table_names.append(t)
        elif isinstance(t, dict):
            if "name" in t:
                table_names.append(t["name"])  # type: ignore[index]
            elif "table" in t:
                table_names.append(t["table"])  # type: ignore[index]

    if not table_names:
        print("No tables found. Response preview:", resp)
        return 1

    for table in table_names[: args.top_n]:
        print(f"Fetching schema for {table}...")
        schema_resp = client.invoke_tool("sql_get_table_schema", {"database": args.database, "table": table, "sample_size": args.sample_size})
        # Expect schema_resp to include keys: columns (list), sample_rows (list)
        columns = schema_resp.get("columns") or schema_resp.get("schema") or []
        samples = schema_resp.get("sample_rows") or schema_resp.get("rows") or []
        write_table_md(out_dir=out_dir, database=args.database, table=table, schema={"columns": columns}, sample_rows=samples)
        print(f"Wrote docs for {table}")

    print(f"Completed. Docs written to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
