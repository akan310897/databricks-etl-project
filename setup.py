# project_setup.py
import json
import os
import subprocess
import sys

def install_dependencies():
    packages = [
        "great-expectations",
        "delta-spark",
        "pytest",
        "pytest-html",
        "openpyxl"
    ]
    print("Installing dependencies...")
    for pkg in packages:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", pkg, "--quiet"]
        )
    print("✅ All dependencies installed")

def load_config(root_dir=None):
    """
    root_dir: absolute path to project root.
              Pass this in from the calling notebook.
              Falls back to cwd if not provided.
    """
    root_dir= "/Workspace/Repos/akkutiwar3108@gmail.com/databricks-etl-project/"

    config_path = os.path.join(root_dir, "config/config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"config.json not found at: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    print(f"✅ Config loaded from : {config_path}")
    print(f"   catalog={config['dev']['catalog']} | schema={config['dev']['schema']}")
    return config


def get_table(config, layer, name):
    """
    Returns fully qualified table name.
    
    get_table(config, "bronze", "orders")
    → hive_metastore.etl_schema.bronze_orders
    """
    catalog = config["dev"]["catalog"]
    schema  = config["dev"]["schema"]
    table   = config["tables"][layer][name]
    return f"{catalog}.{schema}.{table}"

def get_raw_path(config):
    path = config["dev"]["paths"]
    
    return path

def get_all_tables(config):
    """
    Returns a flat dict of all fully qualified table names.
    Mirrors your original constants exactly.

    {
      "BRONZE_PRODUCTS":  "hive_metastore.etl_schema.bronze_products",
      "BRONZE_CUSTOMERS": "hive_metastore.etl_schema.bronze_customers",
      ...
    }
    """
    catalog = config["dev"]["catalog"]
    schema  = config["dev"]["schema"]

    tables = {}
    for layer, entries in config["dev"]["tables"].items():
        for name, table in entries.items():
            key = f"{layer.upper()}_{name.upper()}"        # e.g. BRONZE_ORDERS
            tables[key] = f"{catalog}.{schema}.{table}"

    return tables


install_dependencies()





