// example: how to run this script
//  node compare_csv.js src/student.csv src/student_new.csv src/student_differences.csv

import fs from 'fs';
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';

// Function to compare rows ignoring first two columns
function compareRows(row1, row2) {
    // Skip first two columns and compare the rest
    for (let i = 2; i < row1.length; i++) {
        if (row1[i] !== row2[i]) {
            return false;
        }
    }
    return true;
}

// Main function to compare CSV files
function compareCSVFiles(csv1Path, csv2Path, outputPath) {
    try {
        // Read and parse both CSV files
        const csv1Content = fs.readFileSync(csv1Path, 'utf-8');
        const csv2Content = fs.readFileSync(csv2Path, 'utf-8');

        const csv1Data = parse(csv1Content, {
            columns: true,
            skip_empty_lines: true
        });

        const csv2Data = parse(csv2Content, {
            columns: true,
            skip_empty_lines: true
        });

        // Get headers from the first file
        const headers = Object.keys(csv1Data[0]);

        // Find differences
        const differences = [];
        for (const row2 of csv2Data) {
            let found = false;
            for (const row1 of csv1Data) {
                if (compareRows(Object.values(row1), Object.values(row2))) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                differences.push(row2);
            }
        }

        // Write differences to output file
        if (differences.length > 0) {
            const output = stringify(differences, {
                header: true,
                columns: headers
            });
            fs.writeFileSync(outputPath, output);
            console.log(`Found ${differences.length} differences. Written to ${outputPath}`);
        } else {
            console.log('No differences found.');
        }

    } catch (error) {
        console.error('Error:', error.message);
        process.exit(1);
    }
}

// Check command line arguments
if (process.argv.length !== 5) {
    console.log('Usage: node compare_csv.js <csv1_path> <csv2_path> <output_path>');
    process.exit(1);
}

const csv1Path = process.argv[2];
const csv2Path = process.argv[3];
const outputPath = process.argv[4];

compareCSVFiles(csv1Path, csv2Path, outputPath); 