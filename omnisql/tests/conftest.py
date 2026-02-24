"""Shared pytest configuration for omnisql tests."""
import pytest


# Use asyncio mode for all async tests
def pytest_collection_modifyitems(items):
    for item in items:
        if item.get_closest_marker("asyncio") is None:
            if asyncio_marker := next(
                (m for m in item.iter_markers() if m.name == "asyncio"), None
            ):
                pass  # already marked
