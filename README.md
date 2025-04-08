# Steps to execute

# --mask to toggle masking
node process-student.js --mask
node process-parents.js --mask

# split the file using row count
node split-csv-files.js processed_parent_data_fixed_masked.csv --rows 10000