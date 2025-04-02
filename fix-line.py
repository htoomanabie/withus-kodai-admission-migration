def concat_phase1_lines(input_file='combined_student_data.csv', output_file=None):
    if output_file is None:
        output_file = input_file.replace('.csv', '_fixed.csv')
    
    try:
        # Explicitly use UTF-8 encoding for reading
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        processed_lines = [lines[0]]  # Preserve header
        
        i = 1
        while i < len(lines):
            current_line = lines[i].rstrip('\n')
            
            # Keep concatenating lines until we find a line ending with either "phase1", ",phase1", "\"phase1\"" or ",\"phase1\""
            while i + 1 < len(lines) and not (
                current_line.strip().endswith('phase1') or 
                current_line.strip().endswith(',phase1') or
                current_line.strip().endswith('"phase1"') or
                current_line.strip().endswith(',"phase1"')
            ):
                i += 1
                # Replace newline with space when concatenating
                current_line += ' ' + lines[i].rstrip('\n')
            
            processed_lines.append(current_line + '\n')
            i += 1
        
        # Explicitly use UTF-8 encoding for writing
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(processed_lines)
        
        print(f"Processed {len(lines)-1} input lines (excluding header)")
        print(f"Generated {len(processed_lines)-1} output lines (excluding header)")
        print(f"Output saved to {output_file}")
        return True
    except UnicodeDecodeError as e:
        print(f"UTF-8 encoding error: {e}")
        print("Trying with error handling...")
        # Try again with error handling for problematic UTF-8 characters
        return concat_phase1_lines_with_error_handling(input_file, output_file)
    except Exception as e:
        print(f"Error processing file: {e}")
        return False

def concat_phase1_lines_with_error_handling(input_file='combined_student_data.csv', output_file=None):
    if output_file is None:
        output_file = input_file.replace('.csv', '_fixed.csv')
    
    try:
        # Use error handler for problematic UTF-8 characters
        with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
        
        processed_lines = [lines[0]]  # Preserve header
        
        i = 1
        while i < len(lines):
            current_line = lines[i].rstrip('\n')
            
            # Keep concatenating lines until we find a line ending with either "phase1" or ",phase1"
            while i + 1 < len(lines) and not (current_line.strip().endswith('phase1') or current_line.strip().endswith(',phase1')):
                i += 1
                # Replace newline with space when concatenating
                current_line += ' ' + lines[i].rstrip('\n')
            
            processed_lines.append(current_line + '\n')
            i += 1
        
        # Use UTF-8 encoding for writing with BOM to ensure compatibility
        with open(output_file, 'w', encoding='utf-8-sig') as f:
            f.writelines(processed_lines)
        
        print(f"Processed {len(lines)-1} input lines (excluding header) with error handling")
        print(f"Generated {len(processed_lines)-1} output lines (excluding header)")
        print(f"Output saved to {output_file}")
        return True
    except Exception as e:
        print(f"Error processing file with error handling: {e}")
        return False

# If script is run directly
if __name__ == '__main__':
    import sys
    
    # Allow optional input file as command-line argument
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'combined_student_data.csv'
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    success = concat_phase1_lines(input_file, output_file)
    
    if success:
        print("File processing completed successfully.")
    else:
        print("File processing failed.")
        sys.exit(1)