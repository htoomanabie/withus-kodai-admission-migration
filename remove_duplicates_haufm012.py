#!/usr/bin/env python3
import pandas as pd
import os

def remove_duplicates_by_external_user_id():
    """
    Remove duplicate rows from HAUFM012.csv based on externalUserId column.
    Keep only the first occurrence of each externalUserId.
    """
    
    # File paths
    input_file = "src/HAUFM012.csv"
    output_file = "src/HAUFM012_no_duplicates.csv"
    backup_file = "src/HAUFM012_backup.csv"
    
    print(f"Reading file: {input_file}")
    
    # Read the CSV file
    try:
        df = pd.read_csv(input_file)
        print(f"Original file has {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        
        # Check for duplicates
        duplicates = df.duplicated(subset=['externalUserId'], keep='first')
        duplicate_count = duplicates.sum()
        print(f"Found {duplicate_count} duplicate rows based on externalUserId")
        
        if duplicate_count == 0:
            print("No duplicates found. File is already clean.")
            return
        
        # Create backup
        print(f"Creating backup: {backup_file}")
        df.to_csv(backup_file, index=False)
        
        # Remove duplicates, keeping first occurrence
        df_clean = df.drop_duplicates(subset=['externalUserId'], keep='first')
        
        print(f"After removing duplicates: {len(df_clean)} rows")
        print(f"Removed {len(df) - len(df_clean)} duplicate rows")
        
        # Save the cleaned data
        print(f"Saving cleaned data to: {output_file}")
        df_clean.to_csv(output_file, index=False)
        
        # Show some statistics
        print("\nDuplicate removal summary:")
        print(f"Original rows: {len(df)}")
        print(f"After deduplication: {len(df_clean)}")
        print(f"Duplicates removed: {len(df) - len(df_clean)}")
        
        # Show some examples of removed duplicates
        if duplicate_count > 0:
            print("\nExamples of externalUserId values that had duplicates:")
            duplicate_examples = df[df.duplicated(subset=['externalUserId'], keep=False)]['externalUserId'].value_counts().head(5)
            for user_id, count in duplicate_examples.items():
                print(f"  {user_id}: {count} occurrences (kept first, removed {count-1})")
        
    except FileNotFoundError:
        print(f"Error: File {input_file} not found!")
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    remove_duplicates_by_external_user_id() 