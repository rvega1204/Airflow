from typing import Any

# Package version
__version__ = "0.0.1"

# List of public objects of this module
__all__ = ["sql"]

def get_provider_info() -> dict[str, Any]:
    """
    Returns metadata information about this provider package.
    This information can be used by Airflow or other tools to discover
    and describe the package and its capabilities.
    """
    return {
        "package-name": "my-sdk",  # Name of the package
        "name": "My SDK",  # Human-readable name
        "description": "My SDK is a package that provides a set of tools for building Airflow DAGs.",
        "version": [__version__],  # Version as a list
        "task-decorators": [
            {
                "name": "sql",  # Name of the decorator
                "class-name": "my_sdk.decorators.sql.sql_task"  # Full class path for the decorator
            }
        ]
    }