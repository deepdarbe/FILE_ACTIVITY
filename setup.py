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
        ],
    },
)
