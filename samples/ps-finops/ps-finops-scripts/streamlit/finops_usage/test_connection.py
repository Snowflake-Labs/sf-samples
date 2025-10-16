#!/usr/bin/env python3
"""
Test script to verify Snowflake connection and data availability
for the FinOps Streamlit app.
"""

import pandas as pd
import yaml
from snowflake.snowpark.context import get_active_session

def test_connection():
    """Test the Snowflake connection and basic data availability."""
    try:
        # Get session
        session = get_active_session()
        print("✓ Successfully connected to Snowflake")
        
        # Load config
        with open('snowflake.yml', 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader).get('env')
        
        db_name = config.get('finops_sis_db')
        schema_name = config.get('finops_sis_usage_sc')
        tag_name = config.get('finops_tag_name', 'COST_CENTER')
        
        print(f"✓ Using database: {db_name}, schema: {schema_name}")
        
        # Test views/tables
        test_objects = [
            ('COSTCENTER', 'table'),
            ('COMPUTE_AND_QAS_CC_CURRENCY_DAY', 'view'),
            ('QUERYCOUNTS_CC_DAY', 'view'),
            ('SERVERLESS_TASK_CC_CREDITS_DAY', 'view'),
            ('TABLE_STORAGE_DETAILED', 'table')
        ]
        
        for obj_name, obj_type in test_objects:
            try:
                df = session.table([db_name, schema_name, obj_name]).limit(1).to_pandas()
                print(f"✓ {obj_type.title()} {obj_name} exists and accessible")
                print(f"  Columns: {list(df.columns)}")
            except Exception as e:
                print(f"✗ {obj_type.title()} {obj_name} not accessible: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection()
