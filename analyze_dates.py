import pandas as pd
from datetime import datetime, timedelta

# Read the CSV file
df = pd.read_csv('src/processed_inquiry_data.csv')

# Convert Inquiry_Date__c to datetime
df['Inquiry_Date__c'] = pd.to_datetime(df['Inquiry_Date__c'], errors='coerce')

print("=" * 50)
print("COMPLETE DATASET ANALYSIS")
print("=" * 50)

# Get the overall date range
min_date = df['Inquiry_Date__c'].min()
max_date = df['Inquiry_Date__c'].max()

print(f"Overall Date Range:")
print(f"Earliest date: {min_date}")
print(f"Latest date: {max_date}")

# Count total records
total_records = len(df)
print(f"\nTotal records in dataset: {total_records:,}")

# Count null values
null_count = df['Inquiry_Date__c'].isnull().sum()
print(f"Null values: {null_count:,} out of {total_records:,} rows ({(null_count/total_records)*100:.2f}%)")

print("\n" + "=" * 50)
print("LAST 5 YEARS ANALYSIS")
print("=" * 50)

# Calculate date range for last 5 years
current_date = pd.Timestamp.now()
five_years_ago = current_date - pd.DateOffset(years=5)

# Filter data for the last 5 years
mask = (df['Inquiry_Date__c'] >= five_years_ago) & (df['Inquiry_Date__c'] <= current_date)
recent_records = df[mask]

print(f"Analysis for dates between {five_years_ago.strftime('%Y-%m-%d')} and {current_date.strftime('%Y-%m-%d')}:")
print(f"Total records in this period: {len(recent_records):,}")

# Get the date distribution by year
yearly_counts = recent_records['Inquiry_Date__c'].dt.year.value_counts().sort_index()
print("\nRecords by year:")
for year, count in yearly_counts.items():
    print(f"{year}: {count:,} records")

# Calculate percentage of recent records
recent_percentage = (len(recent_records) / total_records) * 100
print(f"\nRecent 5 years records represent {recent_percentage:.2f}% of total dataset") 