import pandas as pd
from collections import Counter
import matplotlib.pyplot as plt

# Read the CSV file
df = pd.read_csv('processed/rehearsal2/logs/student-fail-log.csv')

# Analyze error patterns
error_counts = Counter(df['__Errors'])

# Print total number of records
print(f"\nTotal number of records: {len(df):,}")

# Print error distribution
print("\nError Distribution:")
print("-" * 80)
for error, count in error_counts.most_common():
    percentage = (count / len(df)) * 100
    print(f"Error: {error}")
    print(f"Count: {count:,} ({percentage:.2f}%)")
    print("-" * 80)

# Group errors by their main type
error_types = {}
for error in df['__Errors']:
    if ':' in error:
        main_type = error.split(':')[0]
    else:
        main_type = error[:30]  # fallback for errors without colon
    if main_type not in error_types:
        error_types[main_type] = 0
    error_types[main_type] += 1

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(error_types.keys(), error_types.values(), color='skyblue')
plt.xlabel('Error Type')
plt.ylabel('Count')
plt.title('Error Type Distribution in Log File')
plt.xticks(rotation=30, ha='right')
plt.tight_layout()
plt.show()

print("\nError Types Summary:")
print("-" * 80)
for error_type, errors in error_types.items():
    count = errors
    percentage = (count / len(df)) * 100
    print(f"Error Type: {error_type}")
    print(f"Count: {count:,} ({percentage:.2f}%)")
    print("Sample Error Messages:")
    for error in df[df['__Errors'].str.contains(error_type)][:3]:  # Show first 3 examples
        print(f"  - {error}")
    print("-" * 80) 