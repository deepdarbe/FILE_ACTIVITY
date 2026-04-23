"""FILE ACTIVITY - Setup script."""

from setuptools import setup, find_packages

setup(
    name="file-activity",
    version="1.0.0",
    description="Windows Dosya Paylaşım Analiz ve Arşivleme Sistemi",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[
        "click>=8.0",
        "pyyaml>=6.0",
        "fastapi>=0.100",
        "uvicorn>=0.22",
        "apscheduler>=3.10",
        "pydantic>=2.0",
        "pywin32>=306",
    ],
    entry_points={
        "console_scripts": [
            "file-activity=main:cli",
            # Issue #65 — MCP server. Requires the optional `mcp` and
            # `httpx` extras (see requirements-mcp.txt). Installed
            # unconditionally because the package is small and missing
            # deps fail loudly with a clear ImportError at first use.
            "file-activity-mcp=src.mcp_server.server:main",
        ],
    },
    extras_require={
        "mcp": [
            "mcp>=1.0.0",
            "httpx>=0.27.0",
        ],
    },
)
