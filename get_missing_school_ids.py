#!/usr/bin/env python3
"""
Extract missing school IDs with their occurrence counts from the school staff error log
"""

import re
import pandas as pd
from collections import Counter

def get_missing_school_ids(file_path):
    """Extract all missing school IDs with their counts"""
    
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Extract school IDs from error messages
    school_ids = []
    for error in df['__Errors'].tolist():
        match = re.search(r'Foreign key external ID: (\d+) not found', error)
        if match:
            school_ids.append(match.group(1))
    
    # Count occurrences
    school_id_counts = Counter(school_ids)
    
    # Sort by count (descending)
    sorted_counts = school_id_counts.most_common()
    
    print("=== Missing School IDs with Occurrence Counts ===")
    print("School ID | Count")
    print("-" * 20)
    
    for school_id, count in sorted_counts:
        print(f"{school_id:>9} | {count:>5}")
    
    print(f"\nTotal unique missing school IDs: {len(sorted_counts)}")
    print(f"Total failed records: {len(school_ids)}")
    
    # Save to CSV for further analysis
    output_file = "missing_school_ids.csv"
    with open(output_file, 'w', newline='') as csvfile:
        csvfile.write("School_ID,Count\n")
        for school_id, count in sorted_counts:
            csvfile.write(f"{school_id},{count}\n")
    
    print(f"\nDetailed list saved to: {output_file}")
    
    return sorted_counts

if __name__ == "__main__":
    file_path = "/Users/htoomaung/Repository/withus-kodai-admission-migration/processed/rehearsal3/log/school-staff-fail-log.csv"
    missing_schools = get_missing_school_ids(file_path) 