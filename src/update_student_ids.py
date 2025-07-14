import pandas as pd

# Read the CSV files with string dtype to prevent any conversion
print("Reading student-exist.csv...")
student_exist_df = pd.read_csv('/Users/htoomaung/Repository/withus-kodai-admission-migration/src/student-exist.csv', 
                             dtype=str,
                             keep_default_na=False)

# Create a mapping of External User ID to Id
id_mapping = dict(zip(student_exist_df['MANAERP__External_User_Id__c'], student_exist_df['Id']))

# Read the base file in chunks to handle large file
print("Processing base file...")
chunk_size = 10000
output_file = '/Users/htoomaung/Repository/withus-kodai-admission-migration/src/processed_student_data_updated.csv'

# Process the file in chunks, reading all as strings
for chunk in pd.read_csv('/Users/htoomaung/Repository/withus-kodai-admission-migration/src/processed_student_data_fixed_masked.csv', 
                        chunksize=chunk_size,
                        dtype=str,
                        keep_default_na=False):
    # Update the Id column where External User ID matches
    chunk['Id'] = chunk['MANAERP__External_User_Id__c'].map(id_mapping)
    
    # Write the chunk to the output file, ensuring all values are written as strings
    chunk.to_csv(output_file, 
                mode='a', 
                header=not pd.io.common.file_exists(output_file), 
                index=False,
                quoting=1)  # Quote all fields to preserve exact string values

print("Processing complete! Updated file saved as processed_student_data_updated.csv") 