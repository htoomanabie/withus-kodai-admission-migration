import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import path from 'path';

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
} from './mappings.js';

const CHUNK_SIZE = 5000; // Smaller chunk size to reduce memory pressure

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
    // Check if phone or other_phone already exist (important for intermediate file processing)
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
    
    // Add debug logs for student ID 500681506
    const studentId = record.student_id;
    
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
    // Check if main_email already exists in the record (this is key for intermediate file data)
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
            // NEW: Set empty grade values to '-'
            if (!value || (typeof value === 'string' && value.trim() === '')) {
                value = 'a1AHy000006uXmfMAE';
            }
        } else if (column === 'dm_sendable') {
            value = transformDmSendable(value);
        } else if (column === 'pref_id') {
            value = transformPrefecture(value);
        } else if (column === 'zip_cd') {
            value = removeDashes(value);
        }else if (column === 'branch_id' && value) {
            value = transformBranchId(value);
        } else if (column === 'main_school_branch_id' && value) {
            value = transformBranchId(value);
        }
        
        filteredRecord[column] = value;
    });
        
    return filteredRecord;
}

function combineRecord(student, studentInfo, studentHistory) {
    // Create a combined record
    return {
        ...student,
        ...(studentInfo || {}),
        ...(studentHistory || {})
    };
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
                    
                    // Log the header fields to help with debugging
                    console.log(`   ℹ️ Headers in ${filename}: ${results.meta.fields.join(', ')}`);
                    
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

// Process customer data in chunks
async function processCustomerData(customerData, processedStudentMap) {
    try {
        // Initialize tracking counters
        let customerOnlyCount = 0;
        let bothDatasetsCount = 0;
        let processedCount = 0;
        
        // Initialize final output file with headers
        const finalFilename = 'combined_student_data.csv';
        const headers = REQUIRED_COLUMNS.join(',') + '\n';
        await fs.writeFile(finalFilename, headers);
        
        // Process customer data in chunks
        const chunks = _.chunk(customerData.data, CHUNK_SIZE);
        
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            console.log(`\n   Processing customer chunk ${i + 1} of ${chunks.length}`);
            
            const processedChunk = chunk.map(customer => {
                const studentId = String(customer.customer_id);
                const processedStudent = processedStudentMap.get(studentId);
                
                // Track which datasets had this ID
                if (processedStudent) {
                    bothDatasetsCount++;
                    
                    // Create a new record where we only use customer data
                    // for fields that don't exist in the student record
                    const combinedRecord = { ...processedStudent };
                    
                    // Handle email fields specially to ensure proper combination
                    // Preserve both student and customer email data for derivation
                    const studentEmail = getFieldValue(processedStudent, 'email');
                    const studentPortableEmail = getFieldValue(processedStudent, 'portable_email');
                    const customerEmail = getFieldValue(customer, 'email');
                    const customerPortableEmail = getFieldValue(customer, 'portable_email');
                    
                    // Combine email fields, prioritizing non-empty values
                    combinedRecord.email = studentEmail || customerEmail;
                    combinedRecord.portable_email = studentPortableEmail || customerPortableEmail;
                    
                    // For all other fields, only use customer data if student data is empty
                    Object.keys(customer).forEach(key => {
                        // Skip customer_id as we already have student_id
                        if (key === 'customer_id') return;
                        
                        // Skip email fields as we've already handled them
                        if (key === 'email' || key === 'portable_email' || 
                            COLUMN_MAPPINGS['email'].includes(key) || 
                            COLUMN_MAPPINGS['portable_email'].includes(key)) {
                            return;
                        }
                        
                        // Only use customer data if student data is empty/null/undefined
                        if (combinedRecord[key] === undefined || 
                            combinedRecord[key] === null || 
                            combinedRecord[key] === '') {
                            combinedRecord[key] = customer[key];
                        }
                    });
                    
                    // Ensure student_id is set properly
                    combinedRecord.student_id = studentId;
                    
                    return filterColumns(combinedRecord);
                } else {
                    customerOnlyCount++;
                    
                    // For customer-only records, proceed as before
                    const combinedRecord = {
                        ...customer,
                        student_id: customer.customer_id
                    };
                    
                    return filterColumns(combinedRecord);
                }
            });
            
            // Convert chunk to CSV
            const csv = processedChunk.map(record => 
                REQUIRED_COLUMNS.map(column => {
                    const value = record[column];
                    return typeof value === 'string' ? `"${value.replace(/"/g, '""')}"` : (value ?? '');
                }).join(',')
            ).join('\n');
            
            if (processedChunk.length > 0) {
                await fs.appendFile(finalFilename, csv + '\n');
            }
            
            processedCount += processedChunk.length;
            console.log(`   ✓ Processed ${processedCount}/${customerData.data.length} customer records`);
            
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
            processedCount,
            customerOnlyCount,
            bothDatasetsCount
        };
    } catch (error) {
        console.error(`Error processing customer data: ${error.message}`);
        throw error;
    }
}

// Process student data that doesn't exist in customer data
async function processRemainingStudentData(processedStudentMap, customerIds) {
    try {
        // Convert maps to sets for faster lookup
        const customerIdSet = new Set(customerIds.map(id => String(id)));
        let studentOnlyCount = 0;
        
        // Get student IDs that aren't in customer data
        const remainingStudentIds = Array.from(processedStudentMap.keys())
            .filter(id => !customerIdSet.has(id));
        
        if (remainingStudentIds.length === 0) {
            console.log(`   ✓ No remaining student records to process`);
            return studentOnlyCount;
        }
        
        console.log(`\n   Processing ${remainingStudentIds.length} remaining student records`);
        
        // Process in chunks
        const chunks = _.chunk(remainingStudentIds, CHUNK_SIZE);
        
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            console.log(`\n   Processing remaining student chunk ${i + 1} of ${chunks.length}`);
            
            const processedChunk = chunk.map(studentId => {
                const student = processedStudentMap.get(studentId);
                studentOnlyCount++;

                // Process the record
                const filteredRecord = filterColumns(student);

                // Ensure branch_id/main_school_branch_id logic is preserved
                if (student.branch_id !== null) {
                    if (String(student.branch_id) === '43' || String(student.branch_id) === '46') {
                        // Force the main_school_branch_id to be set correctly
                        filteredRecord.main_school_branch_id = student.branch_id;
                        filteredRecord.branch_id = '';
                    }
                }
                
                return filteredRecord;
            });
            
            // Convert chunk to CSV
            const csv = processedChunk.map(record => 
                REQUIRED_COLUMNS.map(column => {
                    const value = record[column];
                    return typeof value === 'string' ? `"${value.replace(/"/g, '""')}"` : (value ?? '');
                }).join(',')
            ).join('\n');
            
            if (processedChunk.length > 0) {
                await fs.appendFile('combined_student_data.csv', csv + '\n');
            }
            
            console.log(`   ✓ Processed ${i * CHUNK_SIZE + processedChunk.length}/${remainingStudentIds.length} remaining student records`);
            
            // Free memory
            processedChunk.length = 0;
            
            // Log memory usage
            const currentMemory = process.memoryUsage();
            console.log(`   Current heap used: ${Math.round(currentMemory.heapUsed / 1024 / 1024)}MB`);
        }
        
        // Free memory
        chunks.length = 0;
        
        return studentOnlyCount;
    } catch (error) {
        console.error(`Error processing remaining student data: ${error.message}`);
        throw error;
    }
}

async function combineStudentData() {
    try {
        console.log('Starting step 1: Student data combination process...');
        const initialMemoryUsage = process.memoryUsage();
        console.log(`Initial memory usage: ${Math.round(initialMemoryUsage.heapUsed / 1024 / 1024)}MB`);

        // Step 1: Read all CSV files
        console.log('\n1. Reading CSV files...');
        console.log('   - Reading student.csv...');
        const studentData = await readAndParseCSV('student.csv');
        console.log(`   ✓ Found ${studentData.data.length} student records`);

        console.log('   - Reading student_info.csv...');
        const studentInfoData = await readAndParseCSV('student_info.csv');
        console.log(`   ✓ Found ${studentInfoData.data.length} student info records`);

        console.log('   - Reading student_info_history.csv...');
        const studentHistoryData = await readAndParseCSV('student_info_history.csv');
        console.log(`   ✓ Found ${studentHistoryData.data.length} history records`);

        // Step 2: Process history records
        console.log('\n2. Processing history records to get latest entries...');
        const latestHistoryRecords = _.chain(studentHistoryData.data)
            .groupBy('student_id')
            .mapValues(group => _.maxBy(group, 'created_at'))
            .values()
            .value();
        console.log(`   ✓ Processed ${latestHistoryRecords.length} unique student histories`);

        // Free up memory
        studentHistoryData.data = [];

        // Create lookup maps for faster access
        const studentInfoMap = new Map();
        studentInfoData.data.forEach(info => {
            if (info.student_id) {
                studentInfoMap.set(String(info.student_id), info);
            }
        });
        console.log(`   ✓ Created lookup map for ${studentInfoMap.size} student info records`);
        
        // Free memory
        studentInfoData.data = [];

        const historyMap = new Map();
        latestHistoryRecords.forEach(history => {
            if (history.student_id) {
                historyMap.set(String(history.student_id), history);
            }
        });
        console.log(`   ✓ Created lookup map for ${historyMap.size} history records`);
        
        // Free memory
        latestHistoryRecords.length = 0;

        // Step 3: Process and write student data in chunks
        console.log('\n3. Processing and writing student data in chunks...');
        const totalRecords = studentData.data.length;
        const chunks = _.chunk(studentData.data, CHUNK_SIZE);
        let processedCount = 0;
        let skippedCount = 0;
        
        // Initialize output file with headers
        const intermediateFilename = 'intermediate_student_data.csv';
        const headers = REQUIRED_COLUMNS.join(',') + '\n';
        await fs.writeFile(intermediateFilename, headers);

        // Process each chunk
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            const startIndex = i * CHUNK_SIZE;
            const endIndex = Math.min((i + 1) * CHUNK_SIZE, totalRecords);
            
            console.log(`\n   Processing student chunk ${i + 1} of ${chunks.length}`);
            console.log(`   Records ${startIndex + 1} to ${endIndex} (${chunk.length} records)`);
            
            const processedChunk = chunk
                .filter(student => {
                    const exists = studentInfoMap.has(String(student.student_id));
                    if (!exists) skippedCount++;
                    return exists;
                })
                .map(student => {
                    const studentId = String(student.student_id);
                    const studentInfo = studentInfoMap.get(studentId);
                    const studentHistory = historyMap.get(studentId);
                    const combinedRecord = combineRecord(student, studentInfo, studentHistory);
                    return filterColumns(combinedRecord);
                });

            // Convert chunk to CSV and append to file
            const csv = processedChunk.map(record => 
                REQUIRED_COLUMNS.map(column => {
                    const value = record[column];
                    return typeof value === 'string' ? `"${value.replace(/"/g, '""')}"` : (value ?? '');
                }).join(',')
            ).join('\n');

            if (processedChunk.length > 0) {
                const targetStudent = processedChunk.find(r => String(r.student_id) === "500681506");
    if (targetStudent) {
        console.log(`[DEBUG] Writing to intermediate file - student 500681506 main_email: "${targetStudent.main_email}"`);
    }
                await fs.appendFile(intermediateFilename, csv + '\n');
            }
            
            processedCount += processedChunk.length;
            const progress = ((processedCount / totalRecords) * 100).toFixed(1);
            console.log(`   ✓ Progress: ${progress}% (${processedCount}/${totalRecords} records)`);
            console.log(`   ✓ Current skipped count: ${skippedCount}`);
            
            // Free memory
            processedChunk.length = 0;
            
            // Log memory usage for monitoring
            const currentMemory = process.memoryUsage();
            console.log(`   Current heap used: ${Math.round(currentMemory.heapUsed / 1024 / 1024)}MB`);
        }

        console.log(`\n   ✓ Successfully saved step 1 data to ${intermediateFilename}`);
        
        // Free memory
        chunks.length = 0;
        studentData.data = [];

        // Print summary of step 1
        console.log('\nStep 1 Summary:');
        console.log('----------------');
        console.log(`Total processed student records: ${processedCount}`);
        console.log(`Skipped records (no student info): ${skippedCount}`);
        
        // Run garbage collection if available
        if (global.gc) {
            console.log('Running garbage collection...');
            global.gc();
        }
        
        // STEP 2: Combine with customer.csv (without filtering)
        console.log('\nStarting step 2: Combining with customer data...');
        
        // Read customer data
        console.log('\n1. Reading customer.csv...');
        let customerData;
        try {
            customerData = await readAndParseCSV('customer.csv');
            console.log(`   ✓ Found ${customerData.data.length} customer records`);
        } catch (error) {
            console.log(`   ⚠️ Could not read customer.csv: ${error.message}`);
            console.log(`   ℹ️ Using only processed student data for final output...`);
            
            // Just rename the intermediate file and exit
            try {
                await fs.rename(intermediateFilename, 'combined_student_data.csv');
                console.log(`\n   ✓ Final result is the same as step 1: combined_student_data.csv`);
                return 'combined_student_data.csv';
            } catch (renameError) {
                console.error(`   ❌ Error renaming file: ${renameError.message}`);
                return intermediateFilename;
            }
        }

        // Read the processed student data back into a map to avoid memory issues
        console.log('\n2. Creating lookup map from processed student data...');
        const processedStudentMap = new Map();
        
        // Read student data in chunks
        const processedData = await readAndParseCSV(intermediateFilename);
        processedData.data.forEach(student => {
            if (student.student_id && String(student.student_id) === "500681506") {
                console.log(`[DEBUG] Reading from intermediate file - student 500681506 main_email: "${student.main_email}"`);
            }
            if (student.student_id) {
                processedStudentMap.set(String(student.student_id), student);
            }
        });
        console.log(`   ✓ Created lookup map for ${processedStudentMap.size} processed student records`);
        
        // Free memory
        processedData.data = [];
        
        // Step 3: Process customer records first
        console.log('\n3. Processing customer data...');
        const customerIds = customerData.data.map(c => c.customer_id);
        const customerResult = await processCustomerData(customerData, processedStudentMap);
        
        // Free memory
        customerData.data = [];
        
        // Step 4: Process remaining student records
        console.log('\n4. Processing remaining student records...');
        const studentOnlyCount = await processRemainingStudentData(processedStudentMap, customerIds);
        
        // Free memory
        processedStudentMap.clear();

        // Print final summary
        console.log('\nFinal Summary:');
        console.log('-------------');
        console.log(`Total processed records in final output: ${customerResult.processedCount + studentOnlyCount}`);
        console.log(`Records only in student data: ${studentOnlyCount}`);
        console.log(`Records only in customer data: ${customerResult.customerOnlyCount}`);
        console.log(`Records in both datasets: ${customerResult.bothDatasetsCount}`);
        
        const finalMemory = process.memoryUsage();
        console.log(`Final memory usage: ${Math.round(finalMemory.heapUsed / 1024 / 1024)}MB`);
        console.log('\nProcess completed successfully! ✨');

        return customerResult.finalFilename;
    } catch (error) {
        console.error('\n❌ Error combining data:', error);
        throw error;
    }
}

async function main() {
    console.log('🚀 Starting student and customer data combination process...\n');
    const startTime = Date.now();
    
    try {
        const outputFilename = await combineStudentData();
        
        // Add this line to rename columns at the end of processing
        await renameColumnsInFinalOutput(outputFilename);
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\nTotal execution time: ${duration} seconds`);
    } catch (error) {
        console.error('\nScript failed:', error);
        process.exit(1);
    }
}

// Add this function to the end of your merge-students.js file
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

// Run the script
main();