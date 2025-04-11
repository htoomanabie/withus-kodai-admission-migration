import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import { createReadStream, createWriteStream } from 'fs';

// Define the required columns for our output
const REQUIRED_COLUMNS = [
    'name1',
    'name2',
    'post',
    'leaved',
    'class_name',
    'school_id',
    'contact_person_id',
    'RecordTypeId'
];

// Define column header mapping for human-readable output
const COLUMN_HEADER_MAPPING = {
    'contact_person_id': 'School Staff External Id',
    'name1': 'First Name',
    'name2': 'Last Name',
    'post': 'Role',
    'leaved': 'Active',
    'class_name': 'Description',
    'school_id': 'School',
    'RecordTypeId': 'Record Type'
};

// Define the mapping for leaved values
const LEAVED_MAPPING = {
    'f': 'FALSE',
    't': 'TRUE'
};

// Function to transform leaved values
function transformLeaved(value) {
    if (typeof value === 'string') {
        const lowerValue = value.toLowerCase();
        return LEAVED_MAPPING[lowerValue] || value;
    }
    return value;
}
function cleanText(value) {
    if (typeof value === 'string') {
        value = value.replace(/[\r\n]+/g, ' ');
        value = value.replace(/\s+/g, ' ');
        value = value.trim();
        return value;
    }
    return value;
}

// Process contact_person.csv file
async function processFiles() {
    try {
        console.log('Starting contact person data processing...');
        
        // Create output file with BOM for UTF-8
        const finalFilename = 'processed-school-staff.csv';
        const outputStream = createWriteStream(finalFilename, { encoding: 'utf8' });
        
        // Write UTF-8 BOM and header row
        outputStream.write('\uFEFF'); // Write UTF-8 BOM
        const headerRow = REQUIRED_COLUMNS.map(column => 
            `"${(COLUMN_HEADER_MAPPING[column] || column).replace(/"/g, '""')}"`
        ).join(',') + '\n';
        outputStream.write(headerRow);
        
        // Set up variables for tracking progress
        let isFirstRow = true;
        let processedCount = 0;
        let columnIndices = {};
        
        // Process contact person data with streaming to save memory
        return new Promise((resolve, reject) => {
            const parser = Papa.parse(createReadStream('contact_person.csv', { encoding: 'utf8' }), {
                header: false, // Process headers manually
                skipEmptyLines: true,
                encoding: 'utf8',
                delimitersToGuess: [',', '\t', '|', ';', Papa.RECORD_SEP, Papa.UNIT_SEP],
                chunk: function(results) {
                    try {
                        const data = results.data;
                        
                        // Handle header row
                        if (isFirstRow) {
                            isFirstRow = false;
                            
                            // Map column names to indices
                            data[0].forEach((header, index) => {
                                // Enhanced header cleaning
                                let cleanHeader = header;
                                // Remove all types of quotes and whitespace
                                cleanHeader = cleanHeader.replace(/^["']|["']$/g, '')  // Remove outer quotes
                                    .replace(/\\"/g, '"')  // Handle escaped quotes
                                    .trim()
                                    .toLowerCase();
                                
                                // Special handling for contact_person_id - it's the first column
                                if (index === 0) {
                                    columnIndices['contact_person_id'] = index;
                                } else {
                                    columnIndices[cleanHeader] = index;
                                }
                            });
                            
                            return; // Skip processing header row
                        }
                        
                        // Process each data row
                        for (let i = 0; i < data.length; i++) {
                            const row = data[i];
                            if (!row || row.length === 0) continue;
                            
                            // Create a record with empty values for all required columns
                            const record = {};
                            REQUIRED_COLUMNS.forEach(column => {
                                record[column] = '';
                            });
                            
                            // Set default value for RecordTypeId
                            record['RecordTypeId'] = '012Hy000004KRWLIA4';
                            
                            // Fill in values from the row based on column indices
                            Object.keys(columnIndices).forEach(column => {
                                const index = columnIndices[column];
                                if (index < row.length) {
                                    let value = row[index];
                                    // Remove quotes if present and ensure UTF-8
                                    if (typeof value === 'string') {
                                        value = value.replace(/^"|"$/g, '').trim();
                                        // Ensure the value is properly encoded
                                        value = Buffer.from(value, 'utf8').toString('utf8');
                                    }
                                    record[column] = value;
                                }
                            });
                            
                            // Clean text fields and apply transformations
                            REQUIRED_COLUMNS.forEach(column => {
                                if (record[column] && typeof record[column] === 'string') {
                                    // Apply special transformation for leaved field
                                    if (column === 'leaved') {
                                        record[column] = transformLeaved(record[column]);
                                    } else {
                                        record[column] = cleanText(record[column]);
                                    }
                                }
                            });
                            
                            // Convert to CSV row
                            const csvRow = REQUIRED_COLUMNS.map(column => {
                                const value = record[column];
                                return typeof value === 'string' ? 
                                    `"${value.replace(/"/g, '""')}"` : 
                                    (value !== null && value !== undefined ? value : '');
                            }).join(',') + '\n';
                            
                            // Write to output file
                            outputStream.write(csvRow);
                            
                            processedCount++;
                            
                            // Log progress every 10,000 records
                            if (processedCount % 10000 === 0) {
                                console.log(`   Processed ${processedCount} contact person records...`);
                            }
                        }
                    } catch (error) {
                        console.error(`Error processing chunk: ${error.message}`);
                    }
                },
                complete: function() {
                    outputStream.end();
                    
                    // Final progress report
                    console.log(`   ✓ Completed processing ${processedCount} contact person records`);
                    
                    console.log('\nFinal Summary:');
                    console.log('-------------');
                    console.log(`Total processed records in final output: ${processedCount}`);
                    console.log(`Output file: ${finalFilename}`);
                    console.log('\nProcess completed successfully! ✨');
                    
                    resolve(finalFilename);
                },
                error: function(error) {
                    outputStream.end();
                    reject(error);
                }
            });
        });
    } catch (error) {
        console.error('\n❌ Error processing data:', error);
        throw error;
    }
}

// Entry point
async function main() {
    console.log('🚀 Starting contact person data processing...\n');
    const startTime = Date.now();
    
    try {
        await processFiles();
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\nTotal execution time: ${duration} seconds`);
    } catch (error) {
        console.error('\nScript failed:', error);
        process.exit(1);
    }
}

// Run the script
main();