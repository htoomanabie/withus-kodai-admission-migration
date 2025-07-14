import pandas as pd

# Read the CSV file
df = pd.read_csv('src/HAUFM001.csv', header=None)

# Convert the last column to string and strip whitespace
df.iloc[:, -1] = df.iloc[:, -1].astype(str).str.strip()

# Filter rows where the last column equals "1" or "1.0"
filtered_df = df[df.iloc[:, -1].isin(["1", "1.0"])]

# Convert any '1.0' in the last column to '1'
filtered_df.iloc[:, -1] = filtered_df.iloc[:, -1].replace('1.0', '1')

print(f"\nFound {len(filtered_df)} rows with value '1' (including both string and float formats)")
print("Results have been saved to 'filtered_rows.csv'")

# Save the filtered results to a new CSV file
filtered_df.to_csv('filtered_rows.csv', index=False, header=False) 