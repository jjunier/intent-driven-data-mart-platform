"""Output models for dbt artifact generation.

``DbtArtifactBundle`` is the single return type of
``application.mart_service.generate_dbt_artifacts``.  It deliberately
keeps dbt artifacts separate from ``MartSpecification.generated_sql``
(which holds raw DDL for direct execution).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DbtArtifactBundle:
    """All dbt files generated from a single ``MartSpecification``.

    Keys in ``fact_models`` and ``dimension_models`` are filenames
    (e.g. ``"fact_orders.sql"``).  Values are the full file contents.

    Attributes
    ----------
    fact_models:
        Mapping of ``{filename: sql_content}`` for every fact table model.
    dimension_models:
        Mapping of ``{filename: sql_content}`` for every dimension table model.
    schema_yml:
        Full contents of the generated ``models/marts/schema.yml`` file.
    sources_yml:
        Full contents of the generated ``models/sources.yml`` file.
        Declares the raw source group so that ``{{ source('raw', ...) }}``
        references in model SQL resolve correctly.
    """

    fact_models: dict[str, str] = field(default_factory=dict)
    dimension_models: dict[str, str] = field(default_factory=dict)
    schema_yml: str = ""
    sources_yml: str = ""

    def all_files(self) -> dict[str, str]:
        """Return all artifacts as a flat ``{relative_path: content}`` mapping.

        Paths follow dbt convention:
        ``models/sources.yml``,
        ``models/marts/facts/<name>.sql``,
        ``models/marts/dimensions/<name>.sql``,
        ``models/marts/schema.yml``.
        """
        files: dict[str, str] = {}
        if self.sources_yml:
            files["models/sources.yml"] = self.sources_yml
        for filename, content in self.fact_models.items():
            files[f"models/marts/facts/{filename}"] = content
        for filename, content in self.dimension_models.items():
            files[f"models/marts/dimensions/{filename}"] = content
        if self.schema_yml:
            files["models/marts/schema.yml"] = self.schema_yml
        return files
