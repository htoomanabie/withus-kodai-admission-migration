import pandas as pd
import re

# Read the CSV file
df = pd.read_csv('staff_export.csv')

# Function to format the external user ID
def format_external_id(id_value):
    if pd.isna(id_value):
        return id_value
    
    # If it already starts with U and has 7 digits, return as is
    if re.match(r'^U\d{7}$', str(id_value)):
        return id_value
    
    # Remove any existing U prefix and leading zeros
    clean_id = str(id_value).lstrip('U0')
    
    # Add U prefix and pad with zeros to make it 7 digits
    formatted_id = f"U{clean_id.zfill(7)}"
    return formatted_id

# Apply the formatting function to the MANAERP__External_User_Id__c column
df['MANAERP__External_User_Id__c'] = df['MANAERP__External_User_Id__c'].apply(format_external_id)

# Save the updated CSV
df.to_csv('staff_export_updated.csv', index=False)
print("CSV file has been updated and saved as 'staff_export_updated.csv'") 