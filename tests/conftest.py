"""Shared pytest configuration and fixtures.

Sets required environment variables before any test module is imported so that
``app.config.Settings`` can be instantiated without a real ``.env`` file.
"""

import os

# Provide a dummy API key so Settings() does not fail during collection.
# Individual tests that exercise the Anthropic client patch it out anyway.
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-placeholder")
