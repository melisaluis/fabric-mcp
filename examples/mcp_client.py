"""Small MCP HTTP client wrapper used by examples.

This module provides a minimal helper to invoke MCP tools exposed over the MCP HTTP adapter.
It is intentionally tiny and dependency-light (uses requests). It performs a POST to the MCP endpoint
and returns the parsed JSON response.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests


class MCPClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 3000, token: Optional[str] = None) -> None:
        self.host = host
        self.port = port
        self.base_url = f"http://{self.host}:{self.port}"
        self.token = token

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def invoke_tool(self, tool_name: str, params: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
        """Invoke an MCP tool via HTTP.

        The MCP HTTP adapter used in this repo expects a JSON body describing the invocation.
        We send a small envelope with the tool name and params. Adjust if your MCP HTTP adapter
        expects a different shape.

        Returns the parsed JSON response.
        """
        url = f"{self.base_url}/mcp"
        payload = {
            "tool": tool_name,
            "params": params,
        }

        # Simple retry/backoff for transient errors
        attempts = 3
        backoff = 1.0
        last_exc: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                resp = requests.post(url, json=payload, headers=self._headers(), timeout=timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as ex:  # pragma: no cover - network errors in examples
                last_exc = ex
                time.sleep(backoff)
                backoff *= 2

        raise RuntimeError(f"Failed to invoke MCP tool {tool_name}: {last_exc}")


def default_client_from_env(host: Optional[str] = None, port: Optional[int] = None) -> MCPClient:
    import os

    h = host or os.environ.get("FABRIC_RTI_HTTP_HOST", "127.0.0.1")
    p = int(port or os.environ.get("FABRIC_RTI_HTTP_PORT", "3000"))
    token = os.environ.get("FABRIC_RTI_MCP_TOKEN")
    return MCPClient(host=h, port=p, token=token)


__all__ = ["MCPClient", "default_client_from_env"]
