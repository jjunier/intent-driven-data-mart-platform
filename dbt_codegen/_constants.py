"""Shared constants for dbt_codegen.

Centralising these values ensures that source() macro references in
model SQL and sources.yml table declarations always stay in sync.
Any change to the raw schema name needs to happen in exactly one place.
"""

# The dbt source group name used for all raw table references.
# Must match the ``name`` field emitted in ``sources.yml``.
RAW_SCHEMA: str = "raw"
