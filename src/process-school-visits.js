// Function to clean text (remove line breaks, normalize whitespace)
function cleanText(value) {
    if (typeof value === 'string') {
        value = value.replace(/[\r\n]+/g, ' ');
        value = value.replace(/\s+/g, ' ');
        value = value.trim();
        return value;
    }
    return value;
}

// Date formatting function
function formatDate(dateValue) {
    if (!dateValue) return '';
    
    try {
        // Handle different date formats
        let date;
        if (typeof dateValue === 'string') {
            // Try parsing as ISO date first
            date = new Date(dateValue);
            
            // If that fails, try parsing as YYYY/MM/DD
            if (isNaN(date.getTime())) {
                const parts = dateValue.split('/');
                if (parts.length === 3) {
                    date = new Date(parts[0], parts[1] - 1, parts[2]);
                }
            }
            
            // If still invalid, try parsing as YYYY-MM-DD
            if (isNaN(date.getTime())) {
                const parts = dateValue.split('-');
                if (parts.length === 3) {
                    date = new Date(parts[0], parts[1] - 1, parts[2]);
                }
            }
        } else {
            date = new Date(dateValue);
        }
        
        if (isNaN(date.getTime())) {
            console.warn(`Warning: Could not parse date ${dateValue}`);
            return dateValue;
        }

        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        
        return `${year}-${month}-${day}`;
    } catch (error) {
        console.warn(`Warning: Error formatting date ${dateValue}: ${error.message}`);
        return dateValue;
    }
}

// Time formatting function
function formatTime(timeValue) {
    if (!timeValue) return '';
    
    try {
        // Handle different time formats
        let time;
        if (typeof timeValue === 'string') {
            // Try parsing as HH:mm:ss
            const parts = timeValue.split(':');
            if (parts.length >= 2) {
                const hours = parts[0].padStart(2, '0');
                const minutes = parts[1].padStart(2, '0');
                const seconds = parts[2] ? parts[2].padStart(2, '0') : '00';
                return `${hours}:${minutes}:${seconds}.000Z`;
            }
        }
        return timeValue;
    } catch (error) {
        console.warn(`Warning: Error formatting time ${timeValue}: ${error.message}`);
        return timeValue;
    }
}

// Define the mapping for contact_type
const CONTACT_TYPE_MAPPING = {
    '1': '訪問',
    '2': '電話'
};// Transform contact_type values
function transformContactType(value) {
    const strValue = String(value);
    return CONTACT_TYPE_MAPPING[strValue] || value;
}// Define the mapping for rank
const RANK_MAPPING = {
    '1': 'Ｓ',
    '2': 'A',
    '3': 'B',
    '4': 'C',
    '5': 'D'
};// Transform rank values
function transformRank(value) {
    const strValue = String(value);
    return RANK_MAPPING[strValue] || value;
}// Define the required columns for the input
const INPUT_COLUMNS = [
    'school_contact_id',
    'staff_ids',
    'contact_person_ids',
    'school_id',
    'date',
    'start_time',
    'end_time',
    'contact_type',
    'contact_purpose_id',
    'rank',
    'comment'
];import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import { createReadStream, createWriteStream } from 'fs';

// Define the mappings for contact_purpose_id based on contact_type
const CONTACT_PURPOSE_MAPPING_A = {
    // Original mappings
    '1': '新年度挨拶',
    '2': '新入生状況報告',
    '3': 'OS・学校説明会案内',
    '4': 'クラス数･担任等の把握',
    '5': '転編入受入案内',
    '6': '教育活動報告',
    '7': 'パンフ･募集要項配布',
    '8': '入試案内',
    '9': '生徒状況報告',
    '10': '新規開拓訪問',
    '11': 'トライアルプログラム案内',
    '12': '進路決定報告',
    '13': 'ステップアッププログラム案内',
    '14': '新年挨拶',
    '15': 'OS参加報告',
    '16': '追加募集案内',
    '17': 'トライアルプログラム参加報告',
    '18': '在校生同行訪問',
    '19': '卒業生同行訪問',
    '20': '在校生単独訪問（ジョブシャドウ）',
    '21': '四者面談',
    '22': '専攻科・専門カレッジ　保育案内',
    '23': '専攻科・専門カレッジ　社会人基礎力案内',
    '24': '専攻科・専門カレッジ　介護案内',
    '25': '高認予備校案内',
    '26': '高認認知促進活動',
    '27': '情報モラル関係',
    
    // New additional mappings
    '28': 'OS参加報告',
    '29': '追加募集案内',
    '30': 'トライアルプログラム参加報告',
    '31': '在校生同行訪問',
    '32': '卒業生同行訪問',
    '56': '在校生単独訪問（ジョブシャドウ）',
    '57': '四者面談',
    '58': '専攻科・専門カレッジ　保育案内',
    '59': '専攻科・専門カレッジ　社会人基礎力案内',
    '60': '専攻科・専門カレッジ　介護案内',
    '61': '高認予備校案内',
    '62': '高認認知促進活動',
    '68': '情報モラル関係'
};

const CONTACT_PURPOSE_MAPPING_B = {
    // Original mappings
    '1': '新年度挨拶',
    '2': '新入生状況報告',
    '3': 'OS・学校説明会案内',
    '4': 'クラス数･担任等の把握',
    '5': '転編入受入案内',
    '6': '教育活動報告',
    '7': '募集要項配布',
    '8': '入試案内',
    '9': '生徒状況報告',
    '10': '新規開拓訪問',
    '11': 'トライアルプログラム案内',
    '12': '進路決定報告',
    '13': 'ステップアッププログラム案内',
    '14': '新年挨拶',
    '15': 'OS参加報告',
    '16': '追加募集案内',
    '17': 'トライアルプログラム参加報告',
    '18': '訪問アポイント',
    '19': '四者面談依頼',
    '20': '専攻科・専門カレッジ　保育案内',
    '21': '専攻科・専門カレッジ　社会人基礎力案内',
    '22': '専攻科・専門カレッジ　介護案内',
    '23': '高認予備校案内',
    '24': '高認認知促進活動',
    
    // New additional mappings
    '33': '新年度挨拶',
    '34': '新入生状況報告',
    '35': 'OS・学校説明会案内',
    '36': 'クラス数･担任等の把握',
    '37': '転編入受入案内',
    '38': '教育活動報告',
    '39': '募集要項配布',
    '40': '入試案内',
    '41': '生徒状況報告',
    '42': '新規開拓訪問',
    '43': 'トライアルプログラム案内',
    '44': '進路決定報告',
    '45': 'ステップアッププログラム案内',
    '46': '新年挨拶',
    '47': 'OS参加報告',
    '48': '追加募集案内',
    '49': 'トライアルプログラム参加報告',
    '50': '訪問アポイント',
    '51': '四者面談依頼',
    '63': '専攻科・専門カレッジ　保育案内',
    '64': '専攻科・専門カレッジ　社会人基礎力案内',
    '65': '専攻科・専門カレッジ　介護案内',
    '66': '高認予備校案内',
    '67': '高認認知促進活動'
};

// Define the columns for the output (with staff_ids and contact_person_ids split into 4 columns each)
const OUTPUT_COLUMNS = [
    'school_contact_id',
    'staff_id_1',
    'staff_id_2',
    'staff_id_3',
    'staff_id_4',
    'contact_person_id_1',
    'contact_person_id_2',
    'contact_person_id_3',
    'contact_person_id_4',
    'school_id',
    'date',
    'start_time',
    'end_time',
    'contact_type',
    'contact_purpose_id',
    'rank',
    'comment'
];

// Define the mapping for the column headers in the output
const COLUMN_HEADER_MAPPING = {
    'school_contact_id': 'School_Visit_External_Id__c',
    'staff_id_1': 'Staff_1__r:Contact:MANAERP__External_User_Id__c',
    'staff_id_2': 'Staff_2__r:Contact:MANAERP__External_User_Id__c',
    'staff_id_3': 'Staff_3__r:Contact:MANAERP__External_User_Id__c',
    'staff_id_4': 'Staff_4__r:Contact:MANAERP__External_User_Id__c',
    'contact_person_id_1': 'School_Staff_1__r:Contact:School_Staff_External_Id__c',
    'contact_person_id_2': 'School_Staff_2__r:Contact:School_Staff_External_Id__c',
    'contact_person_id_3': 'School_Staff_3__r:Contact:School_Staff_External_Id__c',
    'contact_person_id_4': 'School_Staff_4__r:Contact:School_Staff_External_Id__c',
    'school_id': 'School__r:MANAERP__School__c:MANAERP__School_Partner_Id__c',
    'date': 'Visit_Date__c',
    'start_time': 'Start_Time__c',
    'end_time': 'End_Time__c',
    'contact_type': 'Contact_Type__c',
    'contact_purpose_id': 'Contact_Purpose__c',
    'rank': 'Rank__c',
    'comment': 'Remarks__c'
};

// Transform contact_purpose_id values based on contact_type
function transformContactPurpose(value, contactType) {
    const strValue = String(value);
    
    // If contact_type is 1, use mapping set A
    if (contactType === '1') {
        return CONTACT_PURPOSE_MAPPING_A[strValue] || value;
    }
    // If contact_type is 2, use mapping set B
    else if (contactType === '2') {
        return CONTACT_PURPOSE_MAPPING_B[strValue] || value;
    }
    
    // If contact_type is neither 1 nor 2, return the original value
    return value;
}

// Function to format staff ID with 'U' prefix and zero padding
function formatStaffId(value) {
    if (!value) return '';
    // Remove any existing 'U' prefix and non-numeric characters
    const numericPart = value.replace(/[^0-9]/g, '');
    // Pad with zeros to make it 7 digits (since we'll add 'U' prefix)
    const paddedNumber = numericPart.padStart(7, '0');
    // Add 'U' prefix
    return `U${paddedNumber}`;
}

// Process school_contact.csv file
async function processSchoolContactFile() {
    try {
        console.log('Starting school contact data processing...');
        
        // Create output file
        const finalFilename = 'processed-school-visits.csv';
        const outputStream = createWriteStream(finalFilename);
        
        // Write header row
        const headerRow = OUTPUT_COLUMNS.map(column => 
            `"${(COLUMN_HEADER_MAPPING[column] || column).replace(/"/g, '""')}"`
        ).join(',') + '\n';
        outputStream.write(headerRow);
        
        return new Promise((resolve, reject) => {
            let isFirstRow = true;
            let columnIndices = {};
            let processedCount = 0;
            
            const parser = Papa.parse(createReadStream('school_contact.csv'), {
                header: false, // Process headers manually
                skipEmptyLines: true,
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
                                
                                // Special handling for school_contact_id - it's the first column
                                if (index === 0) {
                                    columnIndices['school_contact_id'] = index;
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
                            
                            // Create a record with empty values for all output columns
                            const record = {};
                            OUTPUT_COLUMNS.forEach(column => {
                                record[column] = '';
                            });
                            
                            // First pass: Extract contact_type for later use in contact_purpose_id transformation
                            let contactTypeValue = '';
                            if ('contact_type' in columnIndices && columnIndices['contact_type'] < row.length) {
                                let value = row[columnIndices['contact_type']];
                                if (typeof value === 'string') {
                                    value = value.replace(/^"|"$/g, '').trim();
                                }
                                contactTypeValue = value;
                            }
                            
                            // Second pass: Fill in values from the row based on column indices
                            Object.keys(columnIndices).forEach(column => {
                                const index = columnIndices[column];
                                if (index < row.length) {
                                    let value = row[index];
                                    // Remove quotes if present
                                    if (typeof value === 'string') {
                                        value = value.replace(/^"|"$/g, '').trim();
                                    }
                                    
                                    // Handle special case for staff_ids - split into 4 columns
                                    if (column === 'staff_ids' && value) {
                                        const staffIds = value.split(',').map(id => id.trim());
                                        
                                        // Assign up to 4 staff IDs to separate columns
                                        for (let j = 0; j < Math.min(staffIds.length, 4); j++) {
                                            record[`staff_id_${j + 1}`] = formatStaffId(staffIds[j]);
                                        }
                                    } 
                                    // Handle special case for contact_person_ids - split into 4 columns
                                    else if (column === 'contact_person_ids' && value) {
                                        const contactPersonIds = value.split(',').map(id => id.trim());
                                        
                                        // Assign up to 4 contact person IDs to separate columns
                                        for (let j = 0; j < Math.min(contactPersonIds.length, 4); j++) {
                                            record[`contact_person_id_${j + 1}`] = contactPersonIds[j];
                                        }
                                    } 
                                    // For other columns, apply appropriate transformations
                                    else {
                                        // Special handling for school_contact_id
                                        if (column === 'school_contact_id') {
                                            record[column] = value || '';
                                        }
                                        else if (column === 'date') {
                                            record[column] = formatDate(value);
                                        }
                                        else if (column === 'start_time' || column === 'end_time') {
                                            record[column] = formatTime(value);
                                        }
                                        else if (column === 'contact_type') {
                                            record[column] = transformContactType(value);
                                        } 
                                        else if (column === 'contact_purpose_id') {
                                            record[column] = transformContactPurpose(value, contactTypeValue);
                                        }
                                        else if (column === 'rank') {
                                            record[column] = transformRank(value);
                                        }
                                        else if (column === 'comment') {
                                            record[column] = cleanText(value);
                                        }
                                        else {
                                            record[column] = value;
                                        }
                                    }
                                }
                            });
                            
                            // Clean comment field
                            if (record.comment) {
                                record.comment = cleanText(record.comment);
                            }
                            
                            // Convert to CSV row
                            const csvRow = OUTPUT_COLUMNS.map(column => {
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
                                console.log(`   Processed ${processedCount} school contact records...`);
                            }
                        }
                    } catch (error) {
                        console.error(`Error processing chunk: ${error.message}`);
                    }
                },
                complete: function() {
                    outputStream.end();
                    
                    // Final progress report
                    console.log(`   ✓ Completed processing ${processedCount} school contact records`);
                    
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
    console.log('🚀 Starting school contact data processing...\n');
    const startTime = Date.now();
    
    try {
        await processSchoolContactFile();
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\nTotal execution time: ${duration} seconds`);
    } catch (error) {
        console.error('\nScript failed:', error);
        process.exit(1);
    }
}

// Run the script
main();