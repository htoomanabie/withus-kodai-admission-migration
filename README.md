# Steps to execute

## Student Merge and Processed
node student-direct-merge.js

## Fix lines with next line char for description column value
python3 fix-line.py

## Mask the file
node student-mask.js

## Split the file by number of rows
node split-csv-files.js --rows 10000 