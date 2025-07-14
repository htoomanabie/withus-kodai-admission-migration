import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import { createReadStream, createWriteStream } from 'fs';
import { maskData } from './mask-data.js';
import { PREFECTURE_MAPPING } from './student-mappings.js';

// Define the required columns for our output
const REQUIRED_COLUMNS = [
    'school_id',
    'school_code',
    'school_kbn',
    'school_ownership_kbn',
    'schedule_curlm_id',
    'school_name',
    'school_name_kana',
    'prefectures_cd',
    'zip_cd',
    'address1',
    'address2',
    'address3',
    'schedule_tel'
];

// Define column header mapping for human-readable output
const COLUMN_HEADER_MAPPING = {
    'school_id': 'MANAERP__School_Partner_Id__c',
    'school_code': 'School_Code__c',
    'school_kbn': 'MANAERP__School_Level__c',
    'school_ownership_kbn': 'OperationType__c',
    'schedule_curlm_id': 'School_Classification__c',
    'school_name': 'Name',
    'school_name_kana': 'School_Name_Phonetic__c',
    'prefectures_cd': 'MANAERP__Prefecture__c',
    'zip_cd': 'Postal_Code__c',
    'address1': 'Address_1__c',
    'address2': 'Address_2__c',
    'address3': 'Address_3__c',
    'schedule_tel': 'Phone__c'
};

// Define the mapping for school_kbn values
const SCHOOL_KBN_MAPPING = {
    '大学': '10',
    '専門職大学': '13',
    '大学院': '15',
    '短大': '20',
    '専門職短期大学': '23',
    '高等専門学校': '25',
    '高校': '30',
    '中学校': '40',
    '小学校': '50',
    '幼稚園': '60',
    '各種': '81',
    '専修': '82',
    'その他教育機関': '90',
    'その他幼稚園・大学院等': '91',
    '公共職業能力開発施設等': '95'
};

// Define the mapping for school_ownership_kbn values
const SCHOOL_OWNERSHIP_KBN_MAPPING = {
    '国立': '0',
    '公立': '1',
    '私立': '2',
    '調査不能': '9'
};

// Define the mapping for curlm_id values
const CURLM_ID_MAPPING = {
    '通信制課程' : '1',
    '全日制課程' : '2',
    '定時制課程' : '3',
    '指定なし' : '4'
};

function cleanText(value) {
    if (typeof value === 'string') {
        value = value.replace(/[\r\n]+/g, ' ');
        value = value.replace(/\s+/g, ' ');
        value = value.trim();
        return value;
    }
    return value;
}

// Function to transform school_kbn values
function transformSchoolKbn(value) {
    return SCHOOL_KBN_MAPPING[value] || value;
}

// Function to transform school_ownership_kbn values
function transformSchoolOwnershipKbn(value) {
    return SCHOOL_OWNERSHIP_KBN_MAPPING[value] || value;
}

// Function to transform curlm_id values
function transformCurlmId(value) {
    return CURLM_ID_MAPPING[value] || value;
}

// Function to transform prefecture values
function transformPrefecture(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return PREFECTURE_MAPPING[strValue] || '';
}

// Function to transform phone number to integers only
function transformPhoneNumber(value) {
    if (!value) return '';
    // Remove all non-digit characters
    return value.replace(/\D/g, '');
}

// Process school.csv file
async function processFiles() {
    try {
        console.log('Starting school data processing...');
        
        // Create output file with BOM for UTF-8
        const finalFilename = 'processed-school.csv';
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
        
        // Process school data with streaming to save memory
        return new Promise((resolve, reject) => {
            const parser = Papa.parse(createReadStream('school.csv', { encoding: 'utf8' }), {
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
                                
                                columnIndices[cleanHeader] = index;
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

                                    // Handle nested fields
                                    if (column === 'school_list.address1') {
                                        record['address1'] = value;
                                    } else if (column === 'school_list.address2') {
                                        record['address2'] = value;
                                    } else if (column === 'school_list.address3') {
                                        record['address3'] = value;
                                    } else if (column === 'school_list.school_schedule_list.tel') {
                                        record['schedule_tel'] = value;
                                    } else if (column === 'school_list.school_schedule_list.curlm_id') {
                                        record['schedule_curlm_id'] = value;
                                    } else {
                                        record[column] = value;
                                    }
                                }
                            });
                            
                            // Clean text fields and apply transformations
                            REQUIRED_COLUMNS.forEach(column => {
                                if (record[column] && typeof record[column] === 'string') {
                                    // Apply special transformations for specific fields
                                    if (column === 'school_kbn') {
                                        record[column] = transformSchoolKbn(record[column]);
                                    } else if (column === 'school_ownership_kbn') {
                                        record[column] = transformSchoolOwnershipKbn(record[column]);
                                    } else if (column === 'schedule_curlm_id') {
                                        record[column] = transformCurlmId(record[column]);
                                    } else if (column === 'prefectures_cd') {
                                        record[column] = transformPrefecture(record[column]);
                                    } else if (column === 'schedule_tel') {
                                        record[column] = transformPhoneNumber(record[column]);
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
                                console.log(`   Processed ${processedCount} school records...`);
                            }
                        }
                    } catch (error) {
                        console.error(`Error processing chunk: ${error.message}`);
                    }
                },
                complete: function() {
                    outputStream.end();
                    
                    // Final progress report
                    console.log(`   ✓ Completed processing ${processedCount} school records`);
                    
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
    console.log('🚀 Starting school data processing...\n');
    const startTime = Date.now();
    
    try {
        // Parse command line arguments
        const args = process.argv.slice(2);
        const shouldMask = args.includes('--mask');
        
        // Remove the --mask flag from args to get the input file
        const inputFileArg = args.filter(arg => arg !== '--mask')[0];
        const inputFile = inputFileArg || 'school.csv';
        
        console.log(`Using input file: ${inputFile}`);
        console.log(`Masking enabled: ${shouldMask}`);
        
        const outputFile = await processFiles();
        
        // Mask sensitive data if needed
        if (shouldMask) {
            console.log('\nRunning mask-data.js to mask sensitive information...');
            const result = await maskData(outputFile);
            if (!result.success) {
                throw new Error(`Failed to mask data: ${result.error}`);
            }
            console.log(`\n📊 Masking summary:`);
            console.log(`- Total records processed: ${result.totalRecords}`);
            console.log(`- Columns masked: ${result.maskedColumns.join(', ')}`);
            console.log(`- Output saved to: ${result.outputFile}`);
        } else {
            console.log('\nSkipping data masking (use --mask flag to enable)');
        }
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\nTotal execution time: ${duration} seconds`);
    } catch (error) {
        console.error('\nScript failed:', error);
        process.exit(1);
    }
}

// Run the script
main();
