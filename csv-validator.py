import csv
import sys

def validate_csv(filename):
    """
    Validate CSV file to ensure each row has the correct number of columns.
    
    Args:
        filename (str): Path to the CSV file to validate
    
    Returns:
        tuple: (is_valid, error_details)
            is_valid (bool): Whether the CSV passed all checks
            error_details (list): List of error messages
    """
    try:
        # Track errors
        errors = []
        total_rows = 0
        problematic_rows = []
        
        # Read the CSV file
        with open(filename, 'r', encoding='utf-8') as csvfile:
            # Detect delimiter (comma or tab)
            sample = csvfile.read(1024)
            csvfile.seek(0)
            
            # Determine dialect
            dialect = csv.Sniffer().sniff(sample)
            reader = csv.reader(csvfile, dialect)
            
            # Read header
            try:
                header = next(reader)
                expected_column_count = len(header)
                print(f"Header columns: {expected_column_count}")
                print(f"Header: {header}")
            except Exception as e:
                return False, [f"Error reading header: {e}"]
            
            # Check each row
            for row_num, row in enumerate(reader, start=2):  # start at 2 because header is row 1
                total_rows += 1
                
                # Count actual columns, accounting for empty trailing columns
                actual_column_count = len(row)
                
                # If column count doesn't match expected
                if actual_column_count != expected_column_count:
                    error_msg = (f"Row {row_num}: Incorrect column count. "
                                 f"Expected {expected_column_count}, got {actual_column_count}")
                    errors.append(error_msg)
                    
                    # Store the problematic row for detailed inspection
                    problematic_rows.append({
                        'row_number': row_num,
                        'expected_columns': expected_column_count,
                        'actual_columns': actual_column_count,
                        'row_data': row
                    })
        
        # Generate detailed report
        print("\n--- CSV Validation Report ---")
        print(f"Total rows processed: {total_rows}")
        
        if errors:
            print(f"\n⚠️ Found {len(errors)} rows with column count mismatches:")
            for error in errors[:10]:  # Limit to first 10 errors
                print(error)
            
            print("\nDetailed Problematic Rows:")
            for prob_row in problematic_rows[:5]:  # Limit to first 5 detailed rows
                print(f"\nRow {prob_row['row_number']}:")
                print(f"  Expected columns: {prob_row['expected_columns']}")
                print(f"  Actual columns: {prob_row['actual_columns']}")
                print("  Row data:")
                for col_idx, col_val in enumerate(prob_row['row_data'], start=1):
                    print(f"    Column {col_idx}: '{col_val}'")
        
        # Determine overall validity
        is_valid = len(errors) == 0
        
        return is_valid, errors
    
    except Exception as e:
        return False, [f"Unexpected error: {e}"]

def main():
    # Use command-line argument or default filename
    filename = sys.argv[1] if len(sys.argv) > 1 else 'combined_student_data.csv'
    
    print(f"Validating CSV file: {filename}")
    
    is_valid, errors = validate_csv(filename)
    
    if is_valid:
        print("\n✅ CSV file is valid. All rows have the correct number of columns.")
        sys.exit(0)
    else:
        print("\n❌ CSV file has structural issues.")
        sys.exit(1)

if __name__ == '__main__':
    main()