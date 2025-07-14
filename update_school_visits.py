import pandas as pd

# Read the CSV files
base_file = '/Users/htoomaung/Repository/withus-kodai-admission-migration/processed/rehearsal2/cont/school-visit-cont.csv'
source_file = '/Users/htoomaung/Repository/withus-kodai-admission-migration/src/processed-school-visits.csv'

print("Reading source file...")
source_df = pd.read_csv(source_file)
print("Reading base file...")
base_df = pd.read_csv(base_file)

# Create a mapping dictionary
print("Creating mapping...")
staff_mapping = dict(zip(source_df['School_Visit_External_Id__c'], 
                        source_df['Staff_1__r:Contact:MANAERP__External_User_Id__c']))

# Update the base file
print("Updating base file...")
base_df['Staff_1__r:Contact:MANAERP__External_User_Id__c'] = base_df['School_Visit_External_Id__c'].map(staff_mapping)

# Save the updated file
output_file = '/Users/htoomaung/Repository/withus-kodai-admission-migration/processed/rehearsal2/cont/school-visit-cont-updated.csv'
print(f"Saving updated file to {output_file}...")
base_df.to_csv(output_file, index=False)

print("Done!") 