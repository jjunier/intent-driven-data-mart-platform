"""dbt artifact generation from MartSpecification.

This package transforms a fully validated ``MartSpecification`` into
dbt-compatible project files (model SQL, schema.yml) without any LLM calls.
All generators are pure functions that accept a spec and return strings.
"""
