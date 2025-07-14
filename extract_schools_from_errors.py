#!/usr/bin/env python3
"""
Extract all unique school IDs from the school staff error log
"""

import re
import pandas as pd
from collections import Counter

def extract_schools_from_errors(file_path):
    """Extract all unique school IDs from the error log"""
    
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Extract school IDs from error messages
    school_ids = []
    for error in df['__Errors'].tolist():
        match = re.search(r'Foreign key external ID: (\d+) not found', error)
        if match:
            school_ids.append(match.group(1))
    
    # Get unique school IDs and their counts
    school_id_counts = Counter(school_ids)
    unique_schools = sorted(school_id_counts.keys(), key=lambda x: int(x))
    
    print("=== All Missing Schools from Error Log ===")
    print(f"Total unique schools: {len(unique_schools)}")
    print(f"Total failed records: {len(school_ids)}")
    print()
    
    print("School ID | Count | Impact %")
    print("-" * 30)
    
    total_failures = len(school_ids)
    for school_id in unique_schools:
        count = school_id_counts[school_id]
        impact = (count / total_failures) * 100
        print(f"{school_id:>9} | {count:>5} | {impact:>6.2f}%")
    
    # Save to CSV
    output_file = "all_missing_schools.csv"
    with open(output_file, 'w', newline='') as csvfile:
        csvfile.write("School_ID,Count,Impact_Percentage\n")
        for school_id in unique_schools:
            count = school_id_counts[school_id]
            impact = (count / total_failures) * 100
            csvfile.write(f"{school_id},{count},{impact:.2f}\n")
    
    print(f"\nComplete list saved to: {output_file}")
    
    # Summary statistics
    print(f"\n=== Summary Statistics ===")
    print(f"Schools with 1 failure: {sum(1 for count in school_id_counts.values() if count == 1)}")
    print(f"Schools with 2-5 failures: {sum(1 for count in school_id_counts.values() if 2 <= count <= 5)}")
    print(f"Schools with 6-10 failures: {sum(1 for count in school_id_counts.values() if 6 <= count <= 10)}")
    print(f"Schools with 11+ failures: {sum(1 for count in school_id_counts.values() if count >= 11)}")
    
    # Top 10 most problematic schools
    print(f"\n=== Top 10 Most Problematic Schools ===")
    top_10 = school_id_counts.most_common(10)
    for i, (school_id, count) in enumerate(top_10, 1):
        impact = (count / total_failures) * 100
        print(f"{i:2d}. School ID {school_id}: {count} failures ({impact:.2f}%)")
    
    return unique_schools, school_id_counts

if __name__ == "__main__":
    file_path = "/Users/htoomaung/Repository/withus-kodai-admission-migration/processed/rehearsal3/log/school-staff-fail-log.csv"
    schools, counts = extract_schools_from_errors(file_path) 