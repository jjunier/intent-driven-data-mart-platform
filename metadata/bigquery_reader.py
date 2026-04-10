"""BigQuery schema reader for data warehouse metadata access.

Implements the ``SchemaReader`` protocol (defined in ``metadata.reader``) for
Google BigQuery datasets.  All BigQuery API calls are isolated behind the
``_get_client()`` helper so that the rest of the codebase remains importable
even when ``google-cloud-bigquery`` is not installed.

The module intentionally avoids making any live BigQuery requests — the
``google.cloud.bigquery`` import is deferred until ``read_tables()`` is first
called (or a ``client`` is injected in the constructor).  Unit tests inject a
mock client directly, so no GCP credentials or network access are required
during testing.

Constraints
-----------
- ``sample_values`` collection is not supported (BigQuery scanning costs).
- ``is_primary_key`` is always ``False`` (BigQuery does not enforce PKs in MVP).
- ``row_count`` is populated from ``__TABLES__`` metadata; failures fall back
  to ``None`` rather than aborting the full ``read_tables()`` call.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from metadata.schema import SourceColumn, SourceTable

if TYPE_CHECKING:
    # Imported only for type annotations so that the module stays importable
    # without google-cloud-bigquery installed.
    from google.cloud import bigquery as _bigquery_type  # noqa: F401


# ---------------------------------------------------------------------------
# Connection configuration
# ---------------------------------------------------------------------------


@dataclass
class BigQueryConnectionConfig:
    """Value object that captures everything needed to connect to BigQuery.

    Parameters
    ----------
    project_id:
        GCP project that owns the dataset.
    dataset_id:
        BigQuery dataset to inspect.
    credentials:
        Optional ``google.oauth2.credentials.Credentials`` instance.
        When ``None``, Application Default Credentials (ADC) are used
        automatically by the BigQuery client.
    """

    project_id: str
    dataset_id: str
    credentials: Any | None = field(default=None, repr=False)


# ---------------------------------------------------------------------------
# BigQuerySchemaReader
# ---------------------------------------------------------------------------


class BigQuerySchemaReader:
    """Reads source table metadata from a Google BigQuery dataset.

    Implements the ``SchemaReader`` protocol so that
    ``application.mart_service.propose_mart_from_request`` can accept this
    reader without any changes to the service layer.

    Parameters
    ----------
    config:
        Connection settings (project, dataset, optional credentials).
    client:
        An already-constructed ``google.cloud.bigquery.Client`` instance.
        Pass this in tests to inject a mock instead of hitting real GCP.
        When ``None`` (the default), a client is created lazily on first use.
    include_row_counts:
        When ``True`` (the default), ``row_count`` is populated from the
        ``__TABLES__`` metadata view.  Row-count failures fall back to
        ``None`` and do **not** abort ``read_tables()``.
    """

    def __init__(
        self,
        config: BigQueryConnectionConfig,
        client: Any | None = None,
        include_row_counts: bool = True,
    ) -> None:
        self._config = config
        self._client = client
        self._include_row_counts = include_row_counts

    # ------------------------------------------------------------------
    # Public interface (SchemaReader protocol)
    # ------------------------------------------------------------------

    def read_tables(self) -> list[SourceTable]:
        """Return metadata for every base table in the configured dataset.

        Queries ``INFORMATION_SCHEMA.TABLES`` and
        ``INFORMATION_SCHEMA.COLUMNS`` using the BigQuery client.  Optionally
        also queries ``__TABLES__`` for row counts.

        Returns
        -------
        list[SourceTable]
            One entry per base table found in the dataset, sorted by name.

        Raises
        ------
        ImportError
            If ``google-cloud-bigquery`` is not installed and no mock client
            was injected.
        """
        client = self._get_client()
        table_names = self._list_table_names(client)
        row_counts = self._fetch_row_counts(client) if self._include_row_counts else {}

        return [
            self._build_source_table(client, name, row_counts.get(name))
            for name in table_names
        ]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_client(self) -> Any:
        """Return the injected client or create one from config.

        Raises ``ImportError`` with a clear message if
        ``google-cloud-bigquery`` is not available.
        """
        if self._client is not None:
            return self._client

        try:
            from google.cloud import bigquery  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "google-cloud-bigquery is required to use BigQuerySchemaReader. "
                "Install it with: pip install google-cloud-bigquery"
            ) from exc

        return bigquery.Client(
            project=self._config.project_id,
            credentials=self._config.credentials,
        )

    def _list_table_names(self, client: Any) -> list[str]:
        """Return sorted table names from INFORMATION_SCHEMA.TABLES."""
        project = self._config.project_id
        dataset = self._config.dataset_id

        sql = f"""
            SELECT table_name
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        rows = client.query(sql).result()
        return [row.table_name for row in rows]

    def _fetch_row_counts(self, client: Any) -> dict[str, int]:
        """Return a mapping of table_name → row_count from __TABLES__.

        Returns an empty dict on any failure so callers can treat missing
        row counts as ``None`` rather than raising.
        """
        project = self._config.project_id
        dataset = self._config.dataset_id

        try:
            sql = f"""
                SELECT table_id, row_count
                FROM `{project}.{dataset}.__TABLES__`
            """
            rows = client.query(sql).result()
            return {row.table_id: int(row.row_count) for row in rows}
        except Exception:  # noqa: BLE001
            return {}

    def _build_source_table(
        self,
        client: Any,
        table_name: str,
        row_count: int | None,
    ) -> SourceTable:
        """Query INFORMATION_SCHEMA.COLUMNS and return a SourceTable."""
        project = self._config.project_id
        dataset = self._config.dataset_id

        sql = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        rows = client.query(sql).result()

        columns = [
            SourceColumn(
                name=row.column_name,
                data_type=row.data_type,
                is_nullable=(row.is_nullable.upper() == "YES"),
                is_primary_key=False,  # BigQuery does not enforce PKs (MVP)
                sample_values=[],      # sample collection not supported (cost)
            )
            for row in rows
        ]

        return SourceTable(
            name=table_name,
            schema_name=dataset,
            columns=columns,
            row_count=row_count,
        )
