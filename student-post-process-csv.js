import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import { transformBranchId } from './mappings.js'; // Import the transformation function

// Column names in the combined_student_data.csv
const EXTERNAL_USER_ID_COL = 'External User Id';
const STUDENT_ID_NUMBER_COL = 'Student ID number';
const LAST_NAME_COL = 'Last Name';
const FIRST_NAME_COL = 'First Name';
const LAST_NAME_PHONETIC_COL = 'Last Name (Phonetic)';
const FIRST_NAME_PHONETIC_COL = 'First Name (Phonetic)';
const CURRENT_MAIN_SCHOOL_COL = 'Current Main school';
const CURRENT_CAMPUS_COL = 'Current Campus';

// Function to read and parse a CSV file
async function readAndParseCSV(filename) {
    try {
        console.log(`Reading ${filename}...`);
        const content = await fs.readFile(filename, 'utf8');
        
        return new Promise((resolve, reject) => {
            Papa.parse(content, {
                header: true,
                skipEmptyLines: true,
                complete: (results) => {
                    if (results.errors.length > 0) {
                        console.log(`   ⚠️  Warnings while parsing ${filename}:`, results.errors);
                    }
                    console.log(`   ✓ Successfully parsed ${results.data.length} records from ${filename}`);
                    resolve(results.data);
                },
                error: (error) => {
                    reject(error);
                }
            });
        });
    } catch (error) {
        console.error(`Error reading ${filename}:`, error);
        throw error;
    }
}

// Function to deduplicate student data based on student.csv relationships
async function deduplicateStudentData() {
    try {
        // Step 1: Read student.csv to get student_id and customer_id pairs
        const studentRecords = await readAndParseCSV('student.csv');
        console.log(`Found ${studentRecords.length} records in student.csv`);
        
        // Extract student_id and customer_id pairs
        const studentCustomerPairs = studentRecords.map(record => ({
            student_id: String(record.student_id),
            customer_id: String(record.customer_id)
        })).filter(pair => pair.student_id && pair.customer_id);
        
        console.log(`Found ${studentCustomerPairs.length} valid student_id/customer_id pairs`);
        
        // Step 2: Read the combined student data CSV
        const combinedStudentData = await readAndParseCSV('combined_student_data.csv');
        console.log(`Found ${combinedStudentData.length} records in combined_student_data.csv`);
        
        // Create a map for quick lookups by External User Id
        const recordsByExternalId = new Map();
        combinedStudentData.forEach(record => {
            if (record[EXTERNAL_USER_ID_COL]) {
                record["tag"] = "phase1";
                recordsByExternalId.set(record[EXTERNAL_USER_ID_COL], record);
            }
        });
        
        // Stats tracking
        let replacedCustomerIds = 0;
        let removedDuplicates = 0;
        let processedPairs = 0;
        let updatedBranchIds = 0;
        
        // Records to keep in the final output
        const recordsToKeep = new Set(combinedStudentData);
        
        // Process each student/customer pair
        for (const pair of studentCustomerPairs) {
            processedPairs++;
            
            if (processedPairs % 1000 === 0) {
                console.log(`Processed ${processedPairs}/${studentCustomerPairs.length} pairs...`);
            }
            
            // Get records using student_id and customer_id as External User Id
            const student1 = recordsByExternalId.get(pair.student_id);
            const student2 = recordsByExternalId.get(pair.customer_id);
            
            // Case 1: Only student2 exists (record with customer_id as External User Id)
            if (!student1 && student2) {
                // Replace External User Id with student_id
                student2[EXTERNAL_USER_ID_COL] = pair.student_id;
                replacedCustomerIds++;
            }
            
            // Case 2: Both student1 and student2 exist
            else if (student1 && student2) {
                // Compare the name fields to determine if they are duplicates
                const areNamesSame = 
                    student1[LAST_NAME_COL] === student2[LAST_NAME_COL] &&
                    student1[FIRST_NAME_COL] === student2[FIRST_NAME_COL] &&
                    student1[LAST_NAME_PHONETIC_COL] === student2[LAST_NAME_PHONETIC_COL] &&
                    student1[FIRST_NAME_PHONETIC_COL] === student2[FIRST_NAME_PHONETIC_COL];
                
                // If they are duplicates, keep student1 and remove student2
                if (areNamesSame) {
                    recordsToKeep.delete(student2);
                    removedDuplicates++;
                }
            }
            
            // Case 3: Only student1 exists or neither exists - nothing to do
        }
        
        // Apply branch ID transformation to all records
        const finalRecords = Array.from(recordsToKeep).map(record => {
            // Make a copy of the record to avoid modifying the original
            const updatedRecord = {...record};
            
            // Transform Current Main school (branch_id)
            if (updatedRecord[CURRENT_MAIN_SCHOOL_COL]) {
                const originalValue = updatedRecord[CURRENT_MAIN_SCHOOL_COL];
                const transformedValue = transformBranchId(originalValue);
                
                if (originalValue !== transformedValue) {
                    updatedRecord[CURRENT_MAIN_SCHOOL_COL] = transformedValue;
                    updatedBranchIds++;
                }
            }
            
            // Transform Current Campus (main_school_branch_id)
            if (updatedRecord[CURRENT_CAMPUS_COL]) {
                const originalValue = updatedRecord[CURRENT_CAMPUS_COL];
                const transformedValue = transformBranchId(originalValue);
                
                if (originalValue !== transformedValue) {
                    updatedRecord[CURRENT_CAMPUS_COL] = transformedValue;
                    // Don't increment counter here to avoid double-counting
                }
            }
            
            return updatedRecord;
        });
        
        console.log(`\nDeduplication and mapping summary:`);
        console.log(`Processed pairs: ${processedPairs}`);
        console.log(`External User Ids replaced from customer_id to student_id: ${replacedCustomerIds}`);
        console.log(`Duplicate records removed: ${removedDuplicates}`);
        console.log(`Branch IDs transformed: ${updatedBranchIds}`);
        console.log(`Original record count: ${combinedStudentData.length}`);
        console.log(`New record count: ${finalRecords.length}`);
        
        // Create a backup of the original file
        await fs.copyFile('combined_student_data.csv', 'combined_student_data.csv.bak');
        console.log('Original file backed up to combined_student_data.csv.bak');
        
        // Convert back to CSV
        const newCsv = Papa.unparse(finalRecords);
        
        // Write the deduplicated data
        await fs.writeFile('combined_student_data.csv', newCsv);
        console.log('Deduplicated and mapped data written to combined_student_data.csv');
        
        return {
            originalCount: combinedStudentData.length,
            newCount: finalRecords.length,
            replacedIds: replacedCustomerIds,
            removedDuplicates: removedDuplicates,
            updatedBranchIds: updatedBranchIds
        };
    } catch (error) {
        console.error('Error during deduplication:', error);
        throw error;
    }
}

// Main function
async function main() {
    console.log('🔄 Starting student data deduplication and mapping process...\n');
    const startTime = Date.now();
    
    try {
        const result = await deduplicateStudentData();
        
        // Print summary
        console.log('\n✅ Deduplication and mapping completed successfully!');
        console.log(`Records before: ${result.originalCount}`);
        console.log(`Records after: ${result.newCount}`);
        console.log(`Customer IDs replaced with Student IDs: ${result.replacedIds}`);
        console.log(`Duplicate records removed: ${result.removedDuplicates}`);
        console.log(`Branch IDs transformed: ${result.updatedBranchIds}`);
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\nTotal execution time: ${duration} seconds`);
    } catch (error) {
        console.error('\n❌ Script failed:', error);
        process.exit(1);
    }
}

// Run the script
main();