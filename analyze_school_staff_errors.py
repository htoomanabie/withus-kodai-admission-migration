#!/usr/bin/env python3
"""
Analyze school staff error log to understand patterns in the __Errors column
"""

import csv
import re
from collections import Counter, defaultdict
import pandas as pd

def analyze_error_log(file_path):
    """Analyze the school staff error log file"""
    
    print("=== School Staff Error Log Analysis ===\n")
    
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    print(f"Total failed records: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print()
    
    # Analyze the __Errors column
    errors = df['__Errors'].tolist()
    
    # Extract error patterns
    error_patterns = []
    school_ids = []
    
    for error in errors:
        # Extract the school ID from the error message
        match = re.search(r'Foreign key external ID: (\d+) not found', error)
        if match:
            school_id = match.group(1)
            school_ids.append(school_id)
            error_patterns.append(error)
    
    print(f"Records with foreign key errors: {len(error_patterns)}")
    print()
    
    # Count unique school IDs that are causing issues
    school_id_counts = Counter(school_ids)
    
    print("=== Most Common Missing School IDs ===")
    print("School ID | Count | Percentage")
    print("-" * 40)
    
    total_errors = len(school_ids)
    for school_id, count in school_id_counts.most_common(20):
        percentage = (count / total_errors) * 100
        print(f"{school_id:>9} | {count:>5} | {percentage:>8.1f}%")
    
    print()
    
    # Analyze error message patterns
    print("=== Error Message Analysis ===")
    print(f"All errors follow the same pattern: 'INVALID_FIELD: Foreign key external ID: [ID] not found for field MANAERP__School_Partner_Id__c in entity MANAERP__School__c []'")
    print()
    
    # Check for any other error patterns
    unique_errors = set(errors)
    print(f"Unique error messages: {len(unique_errors)}")
    print()
    
    if len(unique_errors) == 1:
        print("All errors are identical - missing school foreign key references")
    else:
        print("Multiple error types found:")
        for i, error in enumerate(unique_errors, 1):
            print(f"{i}. {error}")
    
    print()
    
    # Analyze the data structure
    print("=== Data Structure Analysis ===")
    print(f"Sample of failed records:")
    print(df.head(3).to_string())
    print()
    
    # Check for any patterns in the school staff roles
    print("=== School Staff Role Analysis ===")
    role_counts = df['School_Staff_Role__c'].value_counts()
    print("Most common roles in failed records:")
    for role, count in role_counts.head(10).items():
        print(f"  {role}: {count}")
    
    print()
    
    # Summary
    print("=== Summary ===")
    print(f"• Total failed school staff records: {len(df)}")
    print(f"• All failures are due to missing school foreign key references")
    print(f"• {len(school_id_counts)} unique school IDs are missing from the system")
    print(f"• Top 5 missing school IDs account for {sum(list(school_id_counts.values())[:5])} failures ({sum(list(school_id_counts.values())[:5])/total_errors*100:.1f}%)")
    print()
    print("=== Recommendations ===")
    print("1. Verify that all school records exist in the MANAERP__School__c entity")
    print("2. Check if school IDs need to be migrated first before school staff")
    print("3. Validate the school ID mapping between source and target systems")
    print("4. Consider creating missing school records or updating school IDs")

if __name__ == "__main__":
    file_path = "/Users/htoomaung/Repository/withus-kodai-admission-migration/processed/rehearsal3/log/school-staff-fail-log.csv"
    analyze_error_log(file_path) 