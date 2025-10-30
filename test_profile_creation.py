#!/usr/bin/env python3
"""
Test script to demonstrate the create_profile_yml function
"""

import yaml

def create_profile_yml(catalog, schema, host, http_path, token):
    """
    Create a profile.yml file with the specified Databricks connection parameters
    """
    profile_data = {
        'p4b_demo_2': {
            'outputs': {
                'dev': {
                    'catalog': catalog,
                    'host': host,
                    'http_path': http_path,
                    'schema': schema,
                    'threads': 1,
                    'token': token,
                    'type': 'databricks'
                }
            },
            'target': 'dev'
        }
    }
    
    # Write to profile.yml file
    with open('profile.yml', 'w') as f:
        yaml.dump(profile_data, f, default_flow_style=False, sort_keys=False)
    
    return profile_data

if __name__ == "__main__":
    # Example usage with sample values
    sample_data = create_profile_yml(
        catalog="hayden",
        schema="insta_cart", 
        host="your-databricks-host.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/your-warehouse-id",
        token="your-databricks-token"
    )
    
    print("Profile.yml created successfully!")
    print("Generated data structure:")
    print(yaml.dump(sample_data, default_flow_style=False, sort_keys=False))

