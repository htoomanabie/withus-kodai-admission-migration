import { promises as fs } from 'fs';
import Papa from 'papaparse';
import path from 'path';

// Define which columns should be masked (same as in mask-data.js)
const MASKED_COLUMNS = [
    'Last Name',
    'First Name',
    'Last Name (Phonetic)',
    'First Name (Phonetic)',
    'LastName',
    'FirstName',
    'MANAERP__Last_Name_Phonetic__c',
    'MANAERP__First_Name_Phonetic__c',
    'Phone',
    'Other Phone',
    'OtherPhone',
    'portable_tel',
    'tel',
    'Email',
    'Parent Email',
    'portable_email',
    'main_email',
    'Sub Email',
    'Sub_Email__c',
    'Postal Code',
    'City',
    'Street 1',
    'Street 2',
    'Web App Log in Id',
    'Student PC address',
    'Student First Name', 
    'Student Last Name',
    'Student First Name (Phonetic)',
    'Student Last Name (Phonetic)',
    'Parent Phone Number',
    'Parent cell phone number',
    'Inquirer\'s First Name',
    'Inquirer\'s First Name (Phonetic)',
    'Inquirer\'s Last Name',
    'Inquirer\'s Last Name (Phonetic)',
    'Parent Family Name',
    'Parent First Name',
    'Parent Family Name(katakana)',
    'Parent First Name(katakana)',
    'MANAERP__Postal_Code__c',
    'MANAERP__Prefecture__c',
    'MANAERP__City__c',
    'MANAERP__Street_1__c',
    'MANAERP__Street_2__c'
];

// Define columns that should never be masked
const NEVER_MASKED_COLUMNS = [
    'MANAERP__External_User_Id__c',
    'parent_id',
    'student_id',
    'customer_id',
    'Id',
    'GenderIdentity',
    'gender',
    'tag',
    '_MANAERP__Tag__c'
];

/**
 * Reads and parses a CSV file
 * @param {string} filePath - Path to the CSV file
 * @returns {Object} - Parsed CSV data with headers and records
 */
async function readCSV(filePath) {
    const content = await fs.readFile(filePath, 'utf8');
    
    return new Promise((resolve, reject) => {
        Papa.parse(content, {
            header: true,
            skipEmptyLines: true,
            dynamicTyping: false,
            transform: (value) => {
                if (typeof value === 'string') {
                    return value.trim();
                }
                return value;
            },
            complete: (results) => {
                // Filter out non-critical parsing warnings
                const criticalErrors = results.errors.filter(error => 
                    error.code !== 'TooFewFields' && 
                    error.code !== 'TooManyFields' &&
                    error.code !== 'InvalidQuotes'
                );
                
                // Show summary for field mismatch warnings instead of detailed list
                const fieldMismatchErrors = results.errors.filter(error => 
                    error.code === 'TooFewFields' || error.code === 'TooManyFields'
                );
                
                if (criticalErrors.length > 0) {
                    console.warn(`Critical parsing errors in ${filePath}:`, criticalErrors);
                }
                
                if (fieldMismatchErrors.length > 0) {
                    console.warn(`Note: ${fieldMismatchErrors.length} rows in ${filePath} have field count mismatches (processed as-is)`);
                }
                
                resolve(results);
            },
            error: (error) => {
                reject(error);
            }
        });
    });
}

function compareRecordCounts(beforeData, afterData) {
    console.log('\n📊 Record Count Comparison:');
    console.log(`├── Before masking: ${beforeData.data.length} records`);
    console.log(`├── After masking:  ${afterData.data.length} records`);
    
    if (beforeData.data.length === afterData.data.length) {
        console.log(`└── ✅ Record count matches - No data loss`);
        return true;
    } else {
        console.log(`└── ❌ Record count mismatch - Data loss detected!`);
        return false;
    }
}

function compareColumnStructure(beforeData, afterData) {
    console.log('\n🏗️  Column Structure Comparison:');
    
    const beforeColumns = beforeData.meta.fields || [];
    const afterColumns = afterData.meta.fields || [];
    
    console.log(`├── Before masking: ${beforeColumns.length} columns`);
    console.log(`├── After masking:  ${afterColumns.length} columns`);
    
    // Check for missing columns
    const missingColumns = beforeColumns.filter(col => !afterColumns.includes(col));
    const extraColumns = afterColumns.filter(col => !beforeColumns.includes(col));
    
    if (missingColumns.length === 0 && extraColumns.length === 0) {
        console.log(`└── ✅ Column structure matches perfectly`);
        return true;
    } else {
        console.log(`└── ❌ Column structure mismatch:`);
        if (missingColumns.length > 0) {
            console.log(`    ├── Missing columns: ${missingColumns.join(', ')}`);
        }
        if (extraColumns.length > 0) {
            console.log(`    └── Extra columns: ${extraColumns.join(', ')}`);
        }
        return false;
    }
}

function analyzeFieldChanges(beforeData, afterData) {
    console.log('\n🔍 Field-by-Field Analysis:');
    
    if (beforeData.data.length === 0 || afterData.data.length === 0) {
        console.log('└── ❌ Cannot analyze - one or both files are empty');
        return false;
    }
    
    const beforeColumns = beforeData.meta.fields || [];
    const commonColumns = beforeColumns.filter(col => afterData.meta.fields.includes(col));
    
    let totalChanges = 0;
    let appropriateChanges = 0;
    let inappropriateChanges = 0;
    let preservedFields = 0;
    
    const columnAnalysis = {};
    
    // Analyze each column
    commonColumns.forEach(column => {
        const changes = {
            changed: 0,
            unchanged: 0,
            shouldBeMasked: MASKED_COLUMNS.includes(column),
            shouldNeverBeMasked: NEVER_MASKED_COLUMNS.includes(column),
            examples: {
                changed: [],
                unchanged: []
            }
        };
        
        // Compare values in this column across all records
        const recordsToCheck = Math.min(beforeData.data.length, afterData.data.length);
        
        for (let i = 0; i < recordsToCheck; i++) {
            const beforeValue = beforeData.data[i][column] || '';
            const afterValue = afterData.data[i][column] || '';
            
            if (beforeValue !== afterValue) {
                changes.changed++;
                totalChanges++;
                
                // Store examples of changes (first 3)
                if (changes.examples.changed.length < 3) {
                    changes.examples.changed.push({
                        before: beforeValue,
                        after: afterValue,
                        recordIndex: i
                    });
                }
                
                // Check if this change is appropriate
                if (changes.shouldBeMasked) {
                    appropriateChanges++;
                } else if (changes.shouldNeverBeMasked) {
                    inappropriateChanges++;
                    console.log(`⚠️  WARNING: Never-masked column "${column}" was changed at record ${i}: "${beforeValue}" → "${afterValue}"`);
                }
            } else {
                changes.unchanged++;
                
                // Store examples of unchanged values (first 2)
                if (changes.examples.unchanged.length < 2 && beforeValue !== '') {
                    changes.examples.unchanged.push({
                        value: beforeValue,
                        recordIndex: i
                    });
                }
                
                if (!changes.shouldBeMasked) {
                    preservedFields++;
                }
            }
        }
        
        columnAnalysis[column] = changes;
    });
    
    // Display summary
    console.log(`├── Total field changes: ${totalChanges}`);
    console.log(`├── Appropriate changes (should be masked): ${appropriateChanges}`);
    console.log(`├── Inappropriate changes (should not be masked): ${inappropriateChanges}`);
    console.log(`└── Preserved fields (correctly unchanged): ${preservedFields}`);
    
    // Display detailed column analysis
    console.log('\n📋 Column-by-Column Analysis:');
    
    Object.entries(columnAnalysis).forEach(([column, analysis]) => {
        const status = analysis.shouldBeMasked ? 
            (analysis.changed > 0 ? '✅ MASKED' : '⚠️  NOT MASKED') :
            (analysis.changed === 0 ? '✅ PRESERVED' : '❌ CHANGED');
            
        console.log(`├── ${column}: ${status}`);
        console.log(`│   ├── Changed: ${analysis.changed}, Unchanged: ${analysis.unchanged}`);
        
        if (analysis.examples.changed.length > 0) {
            console.log(`│   ├── Change examples:`);
            analysis.examples.changed.forEach((example, idx) => {
                const symbol = idx === analysis.examples.changed.length - 1 ? '└──' : '├──';
                console.log(`│   │   ${symbol} Record ${example.recordIndex}: "${example.before}" → "${example.after}"`);
            });
        }
        
        if (analysis.shouldNeverBeMasked && analysis.examples.unchanged.length > 0) {
            console.log(`│   └── Preserved examples: ${analysis.examples.unchanged.map(e => `"${e.value}"`).join(', ')}`);
        }
    });
    
    return inappropriateChanges === 0;
}

function checkDataConsistency(beforeData, afterData) {
    console.log('\n🔐 Data Consistency Checks:');
    
    if (beforeData.data.length === 0 || afterData.data.length === 0) {
        console.log('└── ❌ Cannot check consistency - one or both files are empty');
        return false;
    }
    
    let consistencyIssues = 0;
    const recordsToCheck = Math.min(beforeData.data.length, afterData.data.length, 10); // Check first 10 records
    
    console.log(`├── Checking first ${recordsToCheck} records for consistency...`);
    
    for (let i = 0; i < recordsToCheck; i++) {
        const beforeRecord = beforeData.data[i];
        const afterRecord = afterData.data[i];
        
        // Check that key identifier fields are preserved
        const keyFields = ['MANAERP__External_User_Id__c', 'parent_id', 'student_id', 'customer_id'].filter(field => 
            beforeRecord.hasOwnProperty(field) && afterRecord.hasOwnProperty(field)
        );
        
        keyFields.forEach(field => {
            if (beforeRecord[field] !== afterRecord[field]) {
                console.log(`│   ❌ Record ${i}: Key field "${field}" changed: "${beforeRecord[field]}" → "${afterRecord[field]}"`);
                consistencyIssues++;
            }
        });
        
        // Check that empty fields remain empty (or are appropriately filled)
        Object.keys(beforeRecord).forEach(field => {
            if (afterRecord.hasOwnProperty(field)) {
                const beforeEmpty = !beforeRecord[field] || beforeRecord[field].trim() === '';
                const afterEmpty = !afterRecord[field] || afterRecord[field].trim() === '';
                
                // If field was empty before and is not masked, it should still be empty
                if (beforeEmpty && !afterEmpty && !MASKED_COLUMNS.includes(field)) {
                    console.log(`│   ⚠️  Record ${i}: Empty field "${field}" was populated: "" → "${afterRecord[field]}"`);
                }
            }
        });
    }
    
    if (consistencyIssues === 0) {
        console.log(`└── ✅ Data consistency verified - No key field changes detected`);
        return true;
    } else {
        console.log(`└── ❌ Data consistency issues found: ${consistencyIssues} problems`);
        return false;
    }
}

function generateSummaryReport(beforeFile, afterFile, results) {
    console.log('\n📋 MASKING INTEGRITY REPORT');
    console.log('==========================');
    console.log(`Before file: ${beforeFile}`);
    console.log(`After file:  ${afterFile}`);
    console.log(`Timestamp:   ${new Date().toISOString()}`);
    
    const allPassed = Object.values(results).every(result => result);
    
    console.log(`\n🎯 Overall Result: ${allPassed ? '✅ PASSED' : '❌ FAILED'}`);
    
    console.log('\n📊 Test Results:');
    console.log(`├── Record count integrity: ${results.recordCount ? '✅ PASS' : '❌ FAIL'}`);
    console.log(`├── Column structure integrity: ${results.columnStructure ? '✅ PASS' : '❌ FAIL'}`);
    console.log(`├── Field masking appropriateness: ${results.fieldChanges ? '✅ PASS' : '❌ FAIL'}`);
    console.log(`└── Data consistency: ${results.dataConsistency ? '✅ PASS' : '❌ FAIL'}`);
    
    if (allPassed) {
        console.log('\n🎉 CONCLUSION: Masking process completed successfully with no data loss!');
    } else {
        console.log('\n⚠️  CONCLUSION: Issues detected in masking process. Please review the findings above.');
    }
    
    return allPassed;
}

async function verifyMaskingIntegrity(beforeFile, afterFile) {
    try {
        console.log('🔍 Starting Masking Integrity Verification...\n');
        console.log(`Comparing "${beforeFile}" vs "${afterFile}"`);
        
        // Read both files
        console.log('\n📂 Reading files...');
        const beforeData = await readCSV(beforeFile);
        const afterData = await readCSV(afterFile);
        console.log('✅ Files loaded successfully');
        
        // Run all verification checks
        const results = {
            recordCount: compareRecordCounts(beforeData, afterData),
            columnStructure: compareColumnStructure(beforeData, afterData),
            fieldChanges: analyzeFieldChanges(beforeData, afterData),
            dataConsistency: checkDataConsistency(beforeData, afterData)
        };
        
        // Generate final report
        const success = generateSummaryReport(beforeFile, afterFile, results);
        
        return success;
        
    } catch (error) {
        console.error('\n❌ Error during verification:', error);
        return false;
    }
}

// Main function
async function main() {
    const args = process.argv.slice(2);
    
    if (args.length < 2) {
        console.log('Usage: node verify-masking-integrity.js <before-file> <after-file>');
        console.log('');
        console.log('Example:');
        console.log('  node verify-masking-integrity.js processed_parent_data_fixed.csv processed_parent_data_fixed_masked.csv');
        process.exit(1);
    }
    
    const beforeFile = args[0];
    const afterFile = args[1];
    
    // Check if files exist
    try {
        await fs.access(beforeFile);
        await fs.access(afterFile);
    } catch (error) {
        console.error(`❌ Error: Cannot access files. Please check that both files exist.`);
        console.error(`Before file: ${beforeFile}`);
        console.error(`After file: ${afterFile}`);
        process.exit(1);
    }
    
    const success = await verifyMaskingIntegrity(beforeFile, afterFile);
    
    process.exit(success ? 0 : 1);
}

// Export for use in other scripts
export { verifyMaskingIntegrity };

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
    main();
} 