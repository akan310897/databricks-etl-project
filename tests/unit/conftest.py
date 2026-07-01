# tests/unit/conftest.py
import sys
import os

sys.dont_write_bytecode = True  # Add this at the top

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()



