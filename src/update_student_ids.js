import fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter } from 'csv-writer';

const STUDENT_EXIST_PATH = '/Users/htoomaung/Repository/withus-kodai-admission-migration/src/student-exist.csv';
const BASE_FILE_PATH = '/Users/htoomaung/Repository/withus-kodai-admission-migration/src/processed_student_data_fixed_masked.csv';

// First, read student-exist.csv to create the mapping
console.log('Reading student-exist.csv...');
const idMapping = new Map();

fs.createReadStream(STUDENT_EXIST_PATH)
    .pipe(csv())
    .on('data', (row) => {
        idMapping.set(row['MANAERP__External_User_Id__c'], row['Id']);
    })
    .on('end', () => {
        console.log('Processing base file...');
        
        // Get headers from the input file
        const headers = [];
        fs.createReadStream(BASE_FILE_PATH)
            .pipe(csv())
            .on('data', (row) => {
                if (headers.length === 0) {
                    headers.push(...Object.keys(row));
                    // Add Id column if it doesn't exist
                    if (!headers.includes('Id')) {
                        headers.push('Id');
                    }
                }
            })
            .on('end', () => {
                // Create CSV writer with the same headers and always quote fields
                const csvWriter = createObjectCsvWriter({
                    path: BASE_FILE_PATH,
                    header: headers.map(header => ({ id: header, title: header })),
                    alwaysQuote: true
                });

                const records = [];
                
                // Process the input file
                fs.createReadStream(BASE_FILE_PATH)
                    .pipe(csv())
                    .on('data', (row) => {
                        // Create a new object with all values preserved as strings
                        const newRow = {};
                        for (const key in row) {
                            newRow[key] = row[key];
                        }
                        // Add Id field if it doesn't exist
                        if (!('Id' in newRow)) {
                            newRow['Id'] = '';
                        }
                        // Update the Id field
                        newRow['Id'] = idMapping.get(row['MANAERP__External_User_Id__c']) || '';
                        records.push(newRow);
                    })
                    .on('end', () => {
                        // Write all records back to the base file
                        csvWriter.writeRecords(records)
                            .then(() => {
                                console.log('Processing complete! Updated Id values in the base file.');
                            });
                    });
            });
    }); 