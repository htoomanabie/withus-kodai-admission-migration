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
        console.log('Reading and parsing CSV files...');
        console.time('File Processing');
        
        // Read and parse both CSV files
        const csv1Content = fs.readFileSync(csv1Path, 'utf-8');
        const csv2Content = fs.readFileSync(csv2Path, 'utf-8');

        console.log('Parsing first CSV file...');
        const csv1Data = parse(csv1Content, {
            columns: true,
            skip_empty_lines: true
        });
        console.log(`First file parsed: ${csv1Data.length} rows`);

        console.log('Parsing second CSV file...');
        const csv2Data = parse(csv2Content, {
            columns: true,
            skip_empty_lines: true
        });
        console.log(`Second file parsed: ${csv2Data.length} rows`);

        console.timeEnd('File Processing');

        // Get headers from the first file
        const headers = Object.keys(csv1Data[0]);
        console.log('Headers:', headers);

        // Create a Map for faster lookups from csv1 using External User ID
        console.log('Creating lookup map...');
        console.time('Map Creation');
        const csv1Map = new Map();
        csv1Data.forEach((row) => {
            const externalUserId = row['MANAERP__External_User_Id__c'];
            if (externalUserId) {
                csv1Map.set(externalUserId, row);
            }
        });
        console.timeEnd('Map Creation');

        // Find differences
        console.log('Finding differences...');
        console.time('Comparison');
        const differences = [];
        let processed = 0;
        const total = csv2Data.length;
        const logInterval = Math.floor(total / 10); // Log every 10%

        for (const row2 of csv2Data) {
            const externalUserId = row2['MANAERP__External_User_Id__c'];
            const matchingRow1 = externalUserId ? csv1Map.get(externalUserId) : null;

            if (matchingRow1) {
                // If we found a matching row, compare values
                if (!compareRows(Object.values(matchingRow1), Object.values(row2))) {
                    // If values don't match, create new row with first two columns from csv1
                    const newRow = {
                        ...row2,
                        'MANAERP__Contact_Username_Counter__c': matchingRow1['MANAERP__Contact_Username_Counter__c'],
                        'MANAERP__Contact_Username_Counter__c_1': matchingRow1['MANAERP__Contact_Username_Counter__c_1']
                    };
                    differences.push(newRow);
                }
            } else {
                // If no matching row found, add the row from csv2 as is
                differences.push(row2);
            }

            processed++;
            if (processed % logInterval === 0) {
                console.log(`Progress: ${Math.round((processed / total) * 100)}% (${processed}/${total} rows)`);
            }
        }
        console.timeEnd('Comparison');

        // Write differences to output file
        if (differences.length > 0) {
            console.log(`Writing ${differences.length} differences to output file...`);
            console.time('File Writing');
            const output = stringify(differences, {
                header: true,
                columns: headers
            });
            fs.writeFileSync(outputPath, output);
            console.timeEnd('File Writing');
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

console.log('Starting comparison process...');
console.time('Total Execution');
compareCSVFiles(csv1Path, csv2Path, outputPath);
console.timeEnd('Total Execution'); 