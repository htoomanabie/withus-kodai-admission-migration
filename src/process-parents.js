import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import { fixNextLine } from './fix-line.js';
import { maskData } from './mask-data.js';

const CHUNK_SIZE = 5000; // Smaller chunk size to reduce memory pressure
const TAG_VALUE = "phase22"; // Configurable tag value - can be changed or removed in final migration
// Define the column name mappings for the output
// const OUTPUT_COLUMN_MAPPING = {
//     'parent_id': 'External User Id',
//     'kname1': 'Last Name',
//     'kname2': 'First Name',
//     'fname1': 'Last Name (Phonetic)',
//     'fname2': 'First Name (Phonetic)',
//     'portable_email': 'Email',
//     'email': 'Sub Email',
//     'portable_tel': 'Other Phone',
//     'tel': 'Phone',
//     'zip_cd': 'Postal Code',
//     'pref_id': 'Prefecture',
//     'address1': 'City',
//     'address2': 'Street 1',
//     'address3': 'Street 2',
//     'combined_info': 'Description',
//     'gender': 'Gender Identity',
//     'tag': 'Tag'
// };

const OUTPUT_COLUMN_MAPPING = {
    'parent_id': 'MANAERP__External_User_Id__c',
    'kname1': 'LastName',
    'kname2': 'FirstName',
    'fname1': 'MANAERP__Last_Name_Phonetic__c',
    'fname2': 'MANAERP__First_Name_Phonetic__c',
    'portable_email': 'Email',
    'email': 'Sub_Email__c',
    'portable_tel': 'OtherPhone',
    'tel': 'Phone',
    'zip_cd': 'MANAERP__Postal_Code__c',
    'pref_id': 'MANAERP__Prefecture__c',
    'address1': 'MANAERP__City__c',
    'address2': 'MANAERP__Street_1__c',
    'address3': 'MANAERP__Street_2__c',
    'combined_info': 'Description',
    'gender': 'GenderIdentity',
    'tag': 'MANAERP__Tag__c'
};
// Define the mappings
const PREFECTURE_MAPPING = {
    '1': '北海道',
    '2': '青森県',
    '3': '岩手県',
    '4': '宮城県',
    '5': '秋田県',
    '6': '山形県',
    '7': '福島県',
    '8': '茨城県',
    '9': '栃木県',
    '10': '群馬県',
    '11': '埼玉県',
    '12': '千葉県',
    '13': '東京都',
    '14': '神奈川県',
    '15': '新潟県',
    '16': '富山県',
    '17': '石川県',
    '18': '福井県',
    '19': '山梨県',
    '20': '長野県',
    '21': '岐阜県',
    '22': '静岡県',
    '23': '愛知県',
    '24': '三重県',
    '25': '滋賀県',
    '26': '京都府',
    '27': '大阪府',
    '28': '兵庫県',
    '29': '奈良県',
    '30': '和歌山県',
    '31': '鳥取県',
    '32': '島根県',
    '33': '岡山県',
    '34': '広島県',
    '35': '山口県',
    '36': '徳島県',
    '37': '香川県',
    '38': '愛媛県',
    '39': '高知県',
    '40': '福岡県',
    '41': '佐賀県',
    '42': '長崎県',
    '43': '熊本県',
    '44': '大分県',
    '45': '宮崎県',
    '46': '鹿児島県',
    '47': '沖縄県',
    '99': 'その他'
};

// Define the required columns for our output
const REQUIRED_COLUMNS = [
    'parent_id',
    'kname1',
    'kname2',
    'fname1',
    'fname2',
    'portable_email',
    'email',
    'portable_tel',
    'tel',
    'zip_cd',
    'pref_id',
    'address1',
    'address2',
    'address3',
    'combined_info',
    'gender',
    'tag'
];

// Function to remove dashes from a string
function removeDashes(str) {
    if (!str) return str;
    return String(str).replace(/-/g, '');
}

// Transform prefecture values
function transformPrefecture(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return PREFECTURE_MAPPING[strValue] || value;
}

// Transform and derive phone numbers
function derivePhoneNumbers(record) {
    const tel = record['tel'];
    const portableTel = record['portable_tel'];
    
    let phone = null;
    let otherPhone = null;

    // Use explicit existence checks rather than truthiness
    const hasTel = tel !== undefined && tel !== null && tel !== '';
    const hasPortableTel = portableTel !== undefined && portableTel !== null && portableTel !== '';

    // Both numbers exist
    if (hasTel && hasPortableTel) {
        phone = removeDashes(portableTel);
        otherPhone = removeDashes(tel);
    }
    // Only tel exists
    else if (hasTel) {
        phone = removeDashes(tel);
    }
    // Only portable_tel exists
    else if (hasPortableTel) {
        phone = removeDashes(portableTel);
    }
    // Check if phone or other_phone already exist
    else {
        if (record.phone !== undefined && record.phone !== null && record.phone !== '') {
            phone = record.phone;
        }
        if (record.other_phone !== undefined && record.other_phone !== null && record.other_phone !== '') {
            otherPhone = record.other_phone;
        }
    }

    return {
        phone,
        other_phone: otherPhone
    };
}

function filterColumns(record) {
    const filteredRecord = {};
    
    // Get derived phone numbers
    const phoneNumbers = derivePhoneNumbers(record);
    
    // Create combined info field with line breaks, but only for non-empty values
    const employment = record['employment'] !== undefined ? record['employment'] : '';
    const employmentTel = record['employment_tel'] !== undefined ? record['employment_tel'] : '';
    const comment = record['comment'] !== undefined ? record['comment'] : '';
    
    // Filter out empty values and combine the remaining ones with line breaks
    const valuesToCombine = [employment, employmentTel, comment].filter(value => 
        value !== null && value !== undefined && value !== ''
    );
    
    // Join the non-empty values with forward slashes
    const combinedInfo = valuesToCombine.join('/');
    
    // Create a modified record with the combined field
    const modifiedRecord = {
        ...record,
        combined_info: combinedInfo,
        gender: '不明',
        tag: TAG_VALUE // Alternate between phase1 and ,phase1 based on parent_id
    };
    
    REQUIRED_COLUMNS.forEach(column => {
        let value;
        
        // Handle special columns
        if (column === 'tel') {
            value = phoneNumbers.phone;
        } else if (column === 'portable_tel') {
            value = phoneNumbers.other_phone;
        } else {
            value = modifiedRecord[column] !== undefined ? modifiedRecord[column] : '';
        }
        
        // Apply transformations
        if (column === 'pref_id') {
            value = transformPrefecture(value);
        } else if (column === 'zip_cd') {
            // Remove dashes from zip code
            value = removeDashes(value);
        } else if (column === 'parent_id') {
            // Add 'p' prefix to parent_id
            // if (value !== null && value !== undefined && value !== '') {
            //     value = 'p' + value;
            // }
        }
        
        filteredRecord[column] = value;
    });
    
    return filteredRecord;
}

// Reading CSV with manual stream for memory saving
async function readAndParseCSV(filename) {
    try {
        const content = await fs.readFile(filename, 'utf8');
        return new Promise((resolve, reject) => {
            Papa.parse(content, {
                header: true,
                skipEmptyLines: true,
                dynamicTyping: true,
                delimiter: ",",
                newline: "\n",
                transform: (value) => {
                    if (typeof value === 'string') {
                        return value.replace(/\r$/, '');
                    }
                    return value;
                },
                complete: (results) => {
                    if (results.errors.length > 0) {
                        console.log(`   ⚠️  Warnings while parsing ${filename}:`, results.errors);
                    }
                    resolve(results);
                },
                error: (error) => {
                    reject(error);
                }
            });
        });
    } catch (error) {
        console.error(`\n❌ Error reading ${filename}:`, error);
        throw error;
    }
}

// Process parent data in chunks
async function processParentData(parentData) {
    try {
        // Initialize tracking counters
        let processedCount = 0;
        
        // Initialize final output file with headers
        const finalFilename = 'processed_parent_data.csv';
        const headers = REQUIRED_COLUMNS.join(',') + '\n';
        await fs.writeFile(finalFilename, headers);
        
        // Process parent data in chunks
        const chunks = _.chunk(parentData.data, CHUNK_SIZE);
        
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            console.log(`\n   Processing parent chunk ${i + 1} of ${chunks.length}`);
            
            const processedChunk = chunk.map(parent => {
                // Apply transformations
                return filterColumns(parent);
            });
            
            // Convert chunk to CSV
            const csv = processedChunk.map(record => 
                REQUIRED_COLUMNS.map(column => {
                    const value = record[column];
                    // Always quote the combined_info field and handle any existing quotes
                    if (column === 'combined_info') {
                        return `"${(value ?? '').replace(/"/g, '""')}"`;
                    }
                    // For other fields, quote if they contain special characters or are strings
                    return typeof value === 'string' ? `"${value.replace(/"/g, '""')}"` : (value ?? '');
                }).join(',')
            ).join('\n');
            
            if (processedChunk.length > 0) {
                await fs.appendFile(finalFilename, csv + '\n');
            }
            
            processedCount += processedChunk.length;
            console.log(`   ✓ Processed ${processedCount}/${parentData.data.length} parent records`);
            
            // Free memory
            processedChunk.length = 0;
            
            // Log memory usage
            const currentMemory = process.memoryUsage();
            console.log(`   Current heap used: ${Math.round(currentMemory.heapUsed / 1024 / 1024)}MB`);
        }
        
        // Free memory
        chunks.length = 0;
        
        return {
            finalFilename,
            processedCount
        };
    } catch (error) {
        console.error(`Error processing parent data: ${error.message}`);
        throw error;
    }
}

async function processParentFile(filename) {
    try {
        console.log('Starting parent data processing...');
        const initialMemoryUsage = process.memoryUsage();
        console.log(`Initial memory usage: ${Math.round(initialMemoryUsage.heapUsed / 1024 / 1024)}MB`);

        // Step 1: Read parent CSV file
        console.log('\n1. Reading parent.csv...');
        const parentData = await readAndParseCSV(filename);
        console.log(`   ✓ Found ${parentData.data.length} parent records`);

        // Step 2: Process and transform the data
        console.log('\n2. Processing parent data...');
        const result = await processParentData(parentData);
        
        // Free memory
        parentData.data = [];

        // Print final summary
        console.log('\nFinal Summary:');
        console.log('-------------');
        console.log(`Total processed records in final output: ${result.processedCount}`);
        console.log(`Output file: ${result.finalFilename}`);
        
        const finalMemory = process.memoryUsage();
        console.log(`Final memory usage: ${Math.round(finalMemory.heapUsed / 1024 / 1024)}MB`);
        console.log('\nProcess completed successfully! ✨');

        return result.finalFilename;
    } catch (error) {
        console.error('\n❌ Error processing data:', error);
        throw error;
    }
}

// Function to get output column names
function getOutputColumnNames() {
    return REQUIRED_COLUMNS.map(column => OUTPUT_COLUMN_MAPPING[column] || column);
}

// Function to rename columns in the final output file
async function renameColumnsInFinalOutput(filename) {
    try {
        console.log('\nPerforming final column name transformation...');
        
        // Read the original file with original column names
        const content = await fs.readFile(filename, 'utf8');
        
        // Split into lines
        const lines = content.split('\n');
        
        if (lines.length <= 1) {
            console.log('   ⚠️ File appears to be empty or has only header row. Skipping rename.');
            return;
        }
        
        // Get the header line and split into columns
        const headerLine = lines[0];
        const headers = headerLine.split(',');
        
        // Replace the header line with mapped column names
        const newHeaderLine = getOutputColumnNames().join(',');
        
        // Process each data line to ensure Description is quoted
        const processedLines = lines.map((line, index) => {
            if (index === 0) return newHeaderLine; // Return new header line
            
            const columns = line.split(',');
            const descriptionIndex = headers.indexOf('combined_info');
            if (descriptionIndex !== -1 && columns[descriptionIndex]) {
                // Ensure the Description field is properly quoted
                columns[descriptionIndex] = `"${columns[descriptionIndex].replace(/"/g, '""')}"`;
            }
            return columns.join(',');
        });
        
        // Write back to file
        await fs.writeFile(filename, processedLines.join('\n'), 'utf8');
        
        console.log(`   ✓ Successfully renamed columns in ${filename}`);
    } catch (error) {
        console.error(`   ❌ Error renaming columns: ${error.message}`);
    }
}

async function main() {
    console.log('🚀 Starting parent data processing...\n');
    const startTime = Date.now();
    
    try {
        // Parse command line arguments
        const args = process.argv.slice(2);
        const shouldMask = args.includes('--mask');
        
        // Remove the --mask flag from args to get the input file
        const inputFileArg = args.filter(arg => arg !== '--mask')[0];
        const inputFile = inputFileArg || 'parent.csv';
        
        console.log(`Using input file: ${inputFile}`);
        console.log(`Masking enabled: ${shouldMask}`);
        
        // Step 1: Process parent data
        const outputFilename = await processParentFile(inputFile);
        
        // Step 2: Rename columns
        await renameColumnsInFinalOutput(outputFilename);
        
        // Step 3: Run fix-line.js
        console.log('\nRunning fix-line.js to process the final output...');
        const fixedFilename = fixNextLine(outputFilename);
        if (!fixedFilename) {
            throw new Error('Failed to process file with fix-line.js');
        }

        // Step 4: Run masking as the last step if flag is present
        if (shouldMask) {
            console.log('\nRunning mask-data.js to mask sensitive information...');
            const result = await maskData(fixedFilename);
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