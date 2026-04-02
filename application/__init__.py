"""Application service layer.

This package contains use-case orchestration logic that is independent of any
delivery mechanism (MCP, FastAPI, CLI, etc.).  Each public function in this
package accepts primitive inputs and returns typed domain objects so that
callers can format the result however they need.
"""
