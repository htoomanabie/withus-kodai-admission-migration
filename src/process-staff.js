import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import { fixNextLine } from './fix-line.js';
import { maskData } from './mask-data.js';

// Import mappings and helper functions from student-mappings
import {
    REQUIRED_COLUMNS,
    getFieldValue,
    transformPrefecture,
    transformDmSendable,
    transformSex,
    transformBranchId,
    getOutputColumnNames,
    COLUMN_MAPPINGS
} from './student-mappings.js';

// Configuration
const CHUNK_SIZE = 5000; // Smaller chunk size to reduce memory pressure
const TAG_VALUE = "phase11"; // Configurable tag value - can be changed or removed in final migration
const ADD_TAG_COLUMN = TAG_VALUE !== ""; // Flag to determine if tag column should be added

// Function to remove dashes from a string
function removeDashes(str) {
    if (!str) return str;
    return String(str).replace(/-/g, '');
}

// Date formatting function
function formatDate(dateValue) {
    if (!dateValue) return '';
    
    try {
        const date = new Date(dateValue);
        if (isNaN(date.getTime())) return dateValue;

        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        
        return `${year}-${month}-${day}`;
    } catch (error) {
        console.warn(`Warning: Could not format date ${dateValue}`);
        return dateValue;
    }
}

// Transform and derive phone numbers
function derivePhoneNumbers(record) {
    // Use the helper function to check all possible column names
    const tel = getFieldValue(record, 'tel');
    const portableTel = getFieldValue(record, 'portable_tel');
    
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

// Transform and derive email addresses
function deriveEmails(record) {
    // Use the helper function to check all possible column names
    const email = getFieldValue(record, 'email');
    const portableEmail = getFieldValue(record, 'portable_email');
    
    let mainEmail = null;
    let subEmail = null;

    // Important fix: Use explicit existence checks instead of truthy/falsy
    const hasEmail = email !== undefined && email !== null && email !== '';
    const hasPortableEmail = portableEmail !== undefined && portableEmail !== null && portableEmail !== '';

    // Both emails exist
    if (hasEmail && hasPortableEmail) {
        mainEmail = portableEmail;
        subEmail = email;
    }
    // Only email exists
    else if (hasEmail) {
        mainEmail = email;
    }
    // Only portable_email exists
    else if (hasPortableEmail) {
        mainEmail = portableEmail;
    }
    // Check if main_email already exists in the record
    else if (record.main_email !== undefined && record.main_email !== null && record.main_email !== '') {
        mainEmail = record.main_email;
    }
    
    return {
        main_email: mainEmail,
        sub_email: subEmail
    };
}

export const OUTPUT_COLUMN_MAPPING = {
    'kname1': 'Last Name',
    'kname2': 'First Name',
    'fname1': 'Last Name (Phonetic)',
    'fname2': 'First Name (Phonetic)',
    'email': 'Email',
    'user_id': 'username',
    'staff_id': 'External User Id',
    'code': 'Employee number',
    'sex': 'Gender Identity',
    'birthday': 'Birthdate',
    'staff_type': 'Working Type',
    'position_code': 'Role',
    'tel': 'Phone',
    'portable_tel': 'Portable Phone',
    'leaved': 'Working Status',
    'deleted': 'Working Status',
    'careers_date': 'Start Date',
    'leave_date': 'End Date',
    'comment': 'Description'
};

// Staff-specific required columns
const STAFF_REQUIRED_COLUMNS = [
    'kname1',
    'kname2',
    'fname1',
    'fname2',
    'email',
    'user_id',
    'staff_id',
    'code',
    'sex',
    'birthday',
    'staff_type',
    'position_code',
    'tel',
    'portable_tel',
    'leaved',
    'deleted',
    'careers_date',
    'leave_date',
    'comment'
];

function filterColumns(record) {
    const filteredRecord = {};
    
    // Process each required column
    STAFF_REQUIRED_COLUMNS.forEach(column => {
        let value = record[column] !== undefined ? record[column] : '';
        
        // Apply transformations
        if (column === 'birthday' || column === 'careers_date' || column === 'leave_date') {
            value = formatDate(value);
        } else if (column === 'sex') {
            value = transformSex(value);
        } else if (column === 'tel' || column === 'portable_tel') {
            value = removeDashes(value);
        } else if (column === 'leaved' || column === 'deleted') {
            // Convert to number for comparison
            const leavedValue = Number(record.leaved) || 0;
            const deletedValue = Number(record.deleted) || 0;
            
            // Set Working Status based on leaved and deleted values
            if (leavedValue === 1 || deletedValue === 1) {
                value = 'Resigned';
            } else {
                value = 'Available';
            }
        } else if (column === 'staff_type') {
            // Transform staff_type based on the mapping
            const staffTypeMap = {
                '10': '社員',
                '20': '講師',
                '30': 'スタッフ',
                '40': '添削'
            };
            value = staffTypeMap[value] || value;
        } else if (column === 'user_id') {
            // Append @withusmanabie.com to user_id
            value = value ? `${value}@withusmanabie.com` : '';
        }
        
        filteredRecord[column] = value;
    });
    
    // Add tag if configured
    if (ADD_TAG_COLUMN) {
        filteredRecord.tag = TAG_VALUE;
    }
        
    return filteredRecord;
}

// Reading CSV with manual stream for memory saving
async function readAndParseCSV(filename) {
    try {
        console.log(`Reading ${filename}...`);
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
                    
                    // Log the header fields to help with debugging
                    console.log(`   ℹ️ Headers in ${filename}: ${results.meta.fields.join(', ')}`);
                    console.log(`   ✓ Successfully parsed ${results.data.length} records from ${filename}`);
                    
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

// Main function to process staff data
async function processStaffData(inputFile) {
    try {
        console.log('🚀 Starting staff data processing...\n');
        const startTime = Date.now();
        
        // Log the tag value being used
        console.log(`Using tag value: "${TAG_VALUE}" (ADD_TAG_COLUMN: ${ADD_TAG_COLUMN})`);
        
        // Read and parse the input CSV
        const results = await readAndParseCSV(inputFile);
        const records = results.data;
        
        console.log(`\n📊 Processing ${records.length} staff records...`);
        
        // Process records in chunks to manage memory
        const chunks = _.chunk(records, CHUNK_SIZE);
        let processedRecords = [];
        
        for (let i = 0; i < chunks.length; i++) {
            console.log(`\n   Processing chunk ${i + 1}/${chunks.length}...`);
            const chunk = chunks[i];
            
            // Process each record in the chunk
            const processedChunk = chunk.map(record => {
                // Filter and transform the record
                return filterColumns(record);
            });
            
            processedRecords = processedRecords.concat(processedChunk);
        }
        
        // Generate output filename
        const outputFile = 'processed_staff_data.csv';
        
        // Add tag column to the headers if needed
        const columns = ADD_TAG_COLUMN ? [...STAFF_REQUIRED_COLUMNS, 'tag'] : STAFF_REQUIRED_COLUMNS;
        
        // Convert processed records to CSV
        const csv = Papa.unparse(processedRecords, {
            header: true,
            columns: columns
        });
        
        // Write the output file
        await fs.writeFile(outputFile, csv, 'utf8');
        
        const endTime = Date.now();
        const processingTime = (endTime - startTime) / 1000;
        
        console.log(`\n✅ Successfully processed ${processedRecords.length} staff records in ${processingTime.toFixed(2)} seconds`);
        console.log(`📄 Output file: ${outputFile}`);
        
        return outputFile;
    } catch (error) {
        console.error('\n❌ Error processing staff data:', error);
        throw error;
    }
}

// Function to rename columns in the final output
async function renameColumnsInFinalOutput(filename) {
    try {
        console.log(`\nPerforming final column name transformation for ${filename}...`);
        
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
        
        // Map the column names using OUTPUT_COLUMN_MAPPING
        const newHeaders = headers.map(header => {
            // Remove any quotes from the header
            const cleanHeader = header.replace(/['"]/g, '');
            // Map to new name if it exists in OUTPUT_COLUMN_MAPPING, otherwise keep original
            return OUTPUT_COLUMN_MAPPING[cleanHeader] || cleanHeader;
        });
        
        // Create new content with updated header line
        lines[0] = newHeaders.join(',');
        const newContent = lines.join('\n');
        
        // Write back to file
        await fs.writeFile(filename, newContent, 'utf8');
        
        console.log(`   ✓ Successfully renamed columns in ${filename}`);
    } catch (error) {
        console.error(`   ❌ Error renaming columns: ${error.message}`);
    }
}

// Main function
async function main() {
    try {
        // Parse command line arguments
        const args = process.argv.slice(2);
        const shouldMask = args.includes('--mask');
        
        // Remove the --mask flag from args to get the input file
        const inputFileArg = args.filter(arg => arg !== '--mask')[0];
        const inputFile = inputFileArg || 'staff.csv';
        
        console.log(`Using input file: ${inputFile}`);
        console.log(`Masking enabled: ${shouldMask}`);
        
        const outputFile = await processStaffData(inputFile);
        
        // Fix line endings in the output file
        await fixNextLine(outputFile);
        
        // Rename columns in the final output
        await renameColumnsInFinalOutput(outputFile);
        
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
        
        console.log('\n✨ Staff data processing completed successfully!');
    } catch (error) {
        console.error('\n❌ Staff data processing failed:', error);
        process.exit(1);
    }
}

// Run the main function
main(); 