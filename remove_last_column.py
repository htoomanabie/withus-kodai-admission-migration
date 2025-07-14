import csv

input_file = 'school-visit-const-final.csv'
output_file = 'school-visit-const-correct.csv'

with open(input_file, 'r', encoding='utf-8', newline='') as infile, \
     open(output_file, 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    for row in reader:
        if row:  # skip empty rows
            writer.writerow(row[:-1]) 