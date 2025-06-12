import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import { fixNextLine } from './fix-line.js';
import { maskData } from './mask-data.js';

// Import mappings and helper functions
import {
    REQUIRED_COLUMNS,
    getFieldValue,
    transformPrefecture,
    transformDmSendable, 
    transformGrade,
    transformCourseType,
    transformOperateType,
    transformSex,
    transformBranchId,
    getOutputColumnNames,
    COLUMN_MAPPINGS
} from './student-mappings.js';

// Configuration
const CHUNK_SIZE = 5000; // Smaller chunk size to reduce memory pressure
const TAG_VALUE = "phase22"; // Configurable tag value - can be changed or removed in final migration
const ADD_TAG_COLUMN = TAG_VALUE !== ""; // Flag to determine if tag column should be added
const COUNTER_START = 25000001; // Starting value for the counter column. check from `SELECT Id, MANAERP__Username_Count__c FROM MANAERP__Contact_Username_Counter__c`

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
    const hasTel = tel !== undefined && tel !== null && tel !== '' && tel !== '0';
    const hasPortableTel = portableTel !== undefined && portableTel !== null && portableTel !== '' && portableTel !== '0';

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
        if (record.phone !== undefined && record.phone !== null && record.phone !== '' && record.phone !== '0') {
            phone = record.phone;
        }
        if (record.other_phone !== undefined && record.other_phone !== null && record.other_phone !== '' && record.other_phone !== '0') {
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

function filterColumns(record) {
    const filteredRecord = {};
    
    // Get derived phone numbers and emails
    const phoneNumbers = derivePhoneNumbers(record);
    const emails = deriveEmails(record);
    
    // Process each required column
    REQUIRED_COLUMNS.forEach(column => {
        let value;
        
        // Handle special columns
        if (column === 'phone') {
            value = phoneNumbers.phone;
        } else if (column === 'other_phone') {
            value = phoneNumbers.other_phone;
        } else if (column === 'main_email') {
            value = emails.main_email;
        } else if (column === 'sub_email') {
            value = emails.sub_email;
        } else if (column === 'main_school_branch_id') {
            // For main_school_branch_id, check if branch_id is 43 or 46
            const branchId = record.branch_id;
            if (branchId !== null && (String(branchId) === '43' || String(branchId) === '46')) {
                value = branchId;
            } else {
                // Check if main_school_branch_id already exists in the record
                value = record.main_school_branch_id || '';
            }
        } else if (column === 'branch_id') {
            // For branch_id, only keep value if it's not 43 or 46
            const branchId = record.branch_id;
            if (branchId !== null && (String(branchId) === '43' || String(branchId) === '46')) {
                value = '';
            } else {
                value = branchId;
            }
        } else {
            value = record[column] !== undefined ? record[column] : '';
        }
        
        // Apply transformations
        if (column === 'entrance_date' || column === 'graduate_date' || column === 'birthday') {
            value = formatDate(value);
        } else if (column === 'sex') {
            value = transformSex(value);
        } else if (column === 'operate_type_id') {
            value = transformOperateType(value);
        } else if (column === 'course_type') {
            value = transformCourseType(value);
        } else if (column === 'grade') {
            value = transformGrade(value);
            // Set empty grade values to '-'
            if (!value || (typeof value === 'string' && value.trim() === '')) {
                value = '-';
            }
        } else if (column === 'dm_sendable') {
            value = transformDmSendable(value);
        } else if (column === 'pref_id') {
            value = transformPrefecture(value);
        } else if (column === 'zip_cd') {
            value = removeDashes(value);
        } else if (column === 'branch_id' && value) {
            value = transformBranchId(value);
        } else if (column === 'main_school_branch_id' && value) {
            value = transformBranchId(value);
        } else if (column === 'description') {
            // Escape commas and wrap with double quotes if value contains comma
            if (value && typeof value === 'string' && value.includes(',')) {
                value = `"${value.replace(/"/g, '""')}"`;
            }
        } else if (column === 'firstname' || column === 'lastname') {
            // Replace empty firstname or lastname with "不明"
            if (!value || (typeof value === 'string' && value.trim() === '')) {
                value = '不明';
            }
        }
        
        filteredRecord[column] = value;
    });
        
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

// Main function to process student data
async function processStudentData(inputFile) {
    try {
        console.log('🚀 Starting student data processing...\n');
        const startTime = Date.now();
        
        // Initialize counter
        let currentCounter = COUNTER_START;
        
        // Log the tag value being used
        console.log(`Using tag value: "${TAG_VALUE}" (ADD_TAG_COLUMN: ${ADD_TAG_COLUMN})`);
        
        // Step 1: Read student.csv
        console.log('Step 1: Reading student.csv...');
        const studentData = await readAndParseCSV(inputFile);
        console.log(`Found ${studentData.data.length} records in student.csv`);
        
        // Create a map of student_id to customer_id for quick lookups
        const studentIdToCustomerId = new Map();
        studentData.data.forEach(student => {
            if (student.student_id) {
                studentIdToCustomerId.set(String(student.student_id), String(student.customer_id));
            }
        });
        
        // Step 2: Read student_info.csv
        console.log('\nStep 2: Reading student_info.csv...');
        const studentInfoData = await readAndParseCSV('student_info.csv');
        console.log(`Found ${studentInfoData.data.length} records in student_info.csv`);
        
        // Create a map for quick lookups by student_id
        const studentInfoMap = new Map();
        studentInfoData.data.forEach(info => {
            if (info.student_id) {
                studentInfoMap.set(String(info.student_id), info);
            }
        });
        
        // Free memory
        studentInfoData.data = [];
        
        // Step 3: Read customer.csv
        console.log('\nStep 3: Reading customer.csv...');
        const customerData = await readAndParseCSV('customer.csv');
        console.log(`Found ${customerData.data.length} records in customer.csv`);
        
        // Create a map for quick lookups by customer_id
        const customerMap = new Map();
        customerData.data.forEach(customer => {
            if (customer.customer_id) {
                customerMap.set(String(customer.customer_id), customer);
            }
        });
        
        // Free memory
        customerData.data = [];
        
        // Step 3.5: Read student_info_history.csv
        console.log('\nStep 3.5: Reading student_info_history.csv...');
        const studentHistoryData = await readAndParseCSV('student_info_history.csv');
        console.log(`Found ${studentHistoryData.data.length} records in student_info_history.csv`);
        
        // Process history records to get latest entries
        console.log('   Processing history records to get latest entries...');
        const latestHistoryRecords = _.chain(studentHistoryData.data)
            .groupBy('student_id')
            .mapValues(group => _.maxBy(group, 'created_at'))
            .values()
            .value();
        console.log(`   ✓ Processed ${latestHistoryRecords.length} unique student histories`);
        
        // Create a map for quick lookups by student_id
        const historyMap = new Map();
        latestHistoryRecords.forEach(history => {
            if (history.student_id) {
                historyMap.set(String(history.student_id), history);
            }
        });
        console.log(`   ✓ Created lookup map for ${historyMap.size} history records`);
        
        // Free memory
        studentHistoryData.data = [];
        latestHistoryRecords.length = 0;
        
        // Step 4: Process student data and combine with info
        console.log('\nStep 4: Processing student data and combining with info...');
        
        // Initialize output files with headers
        const outputFilename = 'processed_student_data.csv';
        const incompleteFilename = 'processed_student_data_incomplete.csv';
        
        // Add tag column to the headers if needed
        const headers = ADD_TAG_COLUMN ? [...REQUIRED_COLUMNS, 'tag'] : REQUIRED_COLUMNS;
        const headerLine = headers.join(',') + '\n';
        
        await fs.writeFile(outputFilename, headerLine);
        await fs.writeFile(incompleteFilename, headerLine);
        
        // Process student data in chunks
        const chunks = _.chunk(studentData.data, CHUNK_SIZE);
        let processedCount = 0;
        let studentInfoCount = 0;
        let customerInfoCount = 0;
        let incompleteCount = 0;
        let historyOperateTypeCount = 0;
        
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            console.log(`\n   Processing student chunk ${i + 1} of ${chunks.length}`);
            
            const processedChunk = [];
            const incompleteChunk = [];
            
            chunk.forEach(student => {
                const studentId = String(student.student_id);
                const customerId = studentIdToCustomerId.get(studentId);
                
                // Try to find info in student_info.csv first
                let infoRecord = studentInfoMap.get(studentId);
                
                // If not found in student_info.csv, try customer.csv
                if (!infoRecord && customerId) {
                    infoRecord = customerMap.get(customerId);
                    if (infoRecord) {
                        customerInfoCount++;
                    }
                } else if (infoRecord) {
                    // If we have student_info record, check for null values and fill from customer.csv
                    if (customerId) {
                        const customerRecord = customerMap.get(customerId);
                        if (customerRecord) {
                            // Map the fields from customer.csv to student_info.csv format
                            const fieldMappings = {
                                'birthday': 'birthday',
                                'sex': 'sex',
                                'phone': 'phone',
                                'other_phone': 'other_phone',
                                'main_email': 'main_email',
                                'sub_email': 'sub_email',
                                'description': 'description'
                            };

                            // Fill in null values from customer record
                            Object.entries(fieldMappings).forEach(([studentInfoField, customerField]) => {
                                if (!infoRecord[studentInfoField] && customerRecord[customerField]) {
                                    infoRecord[studentInfoField] = customerRecord[customerField];
                                }
                            });
                        }
                    }
                    studentInfoCount++;
                }
                
                // Check if this is an incomplete record (no info found in either source)
                if (!infoRecord) {
                    // This is an incomplete record
                    incompleteCount++;
                    
                    // Create a record with just the student data and tag
                    const incompleteRecord = {
                        ...student
                    };
                    
                    // Apply transformations
                    const filteredRecord = filterColumns(incompleteRecord);
                    
                    // Add tag if needed
                    if (ADD_TAG_COLUMN) {
                        filteredRecord.tag = TAG_VALUE;
                    }
                    
                    incompleteChunk.push(filteredRecord);
                } else {
                    // Check for operate_type_id in student_info_history.csv
                    const historyRecord = historyMap.get(studentId);
                    if (historyRecord && historyRecord.operate_type_id) {
                        // Apply the operate_type_id from history
                        infoRecord.operate_type_id = historyRecord.operate_type_id;
                        historyOperateTypeCount++;
                    }
                    
                    // Combine student data with info
                    const combinedRecord = {
                        ...student,
                        ...infoRecord
                    };
                    
                    // Ensure student_id is set properly
                    combinedRecord.student_id = studentId;
                    
                    // Apply transformations
                    const filteredRecord = filterColumns(combinedRecord);
                    
                    // Add tag if needed
                    if (ADD_TAG_COLUMN) {
                        filteredRecord.tag = TAG_VALUE;
                    }
                    
                    processedChunk.push(filteredRecord);
                }
            });
            
            // Convert chunks to CSV
            const processedCsv = processedChunk.map(record => {
                const counter = currentCounter++;
                return [counter, ...headers.slice(1).map(column => {
                    const value = record[column];
                    return typeof value === 'string' ? `"${value.replace(/"/g, '""')}"` : (value ?? '');
                })].join(',');
            }).join('\n');
            
            const incompleteCsv = incompleteChunk.map(record => {
                const counter = currentCounter++;
                return [counter, ...headers.slice(1).map(column => {
                    const value = record[column];
                    return typeof value === 'string' ? `"${value.replace(/"/g, '""')}"` : (value ?? '');
                })].join(',');
            }).join('\n');
            
            if (processedChunk.length > 0) {
                await fs.appendFile(outputFilename, processedCsv + '\n');
            }
            
            if (incompleteChunk.length > 0) {
                await fs.appendFile(incompleteFilename, incompleteCsv + '\n');
            }
            
            processedCount += processedChunk.length;
            console.log(`   ✓ Processed ${processedCount}/${studentData.data.length} student records`);
            console.log(`   ✓ Incomplete records: ${incompleteCount}`);
            
            // Free memory
            processedChunk.length = 0;
            incompleteChunk.length = 0;
            
            // Log memory usage
            const currentMemory = process.memoryUsage();
            console.log(`   Current heap used: ${Math.round(currentMemory.heapUsed / 1024 / 1024)}MB`);
        }
        
        // Free memory
        chunks.length = 0;
        studentData.data = [];
        
        // Step 5: Rename columns in the final output
        console.log('\nStep 5: Renaming columns in the final output...');
        await renameColumnsInFinalOutput(outputFilename);
        await renameColumnsInFinalOutput(incompleteFilename);
        
        // Print summary
        console.log('\n✅ Processing completed successfully!');
        console.log(`Total student records processed: ${processedCount}`);
        console.log(`Records with student_info data: ${studentInfoCount}`);
        console.log(`Records with customer data: ${customerInfoCount}`);
        console.log(`Incomplete records (written to ${incompleteFilename}): ${incompleteCount}`);
        console.log(`Records with operate_type_id from history: ${historyOperateTypeCount}`);
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\nTotal execution time: ${duration} seconds`);
        
        return {
            outputFilename,
            incompleteFilename,
            processedCount,
            incompleteCount
        };
    } catch (error) {
        console.error('\n❌ Script failed:', error);
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
        
        // Replace the header line with mapped column names
        const newHeaderLine = ADD_TAG_COLUMN 
            ? getOutputColumnNames().join(',') + ',tag'
            : getOutputColumnNames().join(',');
        
        // Create new content with updated header line
        lines[0] = newHeaderLine;
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
    console.log('🚀 Starting student data processing...\n');
    const startTime = Date.now();
    
    try {
        // Parse command line arguments
        const args = process.argv.slice(2);
        const shouldMask = args.includes('--mask');
        
        // Remove the --mask flag from args to get the input file
        const inputFileArg = args.filter(arg => arg !== '--mask')[0];
        const inputFile = inputFileArg || 'student.csv';
        
        console.log(`Using input file: ${inputFile}`);
        console.log(`Masking enabled: ${shouldMask}`);
        
        // Step 1: Process student data
        const { outputFilename, incompleteFilename } = await processStudentData(inputFile);
        
        // Step 2: Run fix-line.js
        console.log('\nRunning fix-line.js to process the final output...');
        const fixedFilename = fixNextLine(outputFilename);
        if (!fixedFilename) {
            throw new Error('Failed to process file with fix-line.js');
        }

        // Step 3: Run masking as the last step if flag is present
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