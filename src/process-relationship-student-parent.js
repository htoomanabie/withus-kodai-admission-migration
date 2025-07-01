import fs from 'fs';
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import path from 'path';
import { fileURLToPath } from 'url';

// Get __dirname equivalent for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Relationship ID mapping
const relationshipMapping = {
    '1': '父',
    '2': '母',
    '3': '祖父',
    '4': '祖母',
    '5': '兄',
    '6': '姉',
    '7': '弟',
    '8': '妹',
    '9': '息子',
    '10': '娘',
    '11': '孫（男）',
    '12': '孫（女）',
    '99': 'その他'
};

// Column mapping
const columnMapping = {
    '生徒ID': 'MANAERP__From__r:Contact:MANAERP__External_User_Id__c',
    '保護者ID': 'MANAERP__To__r:Contact:MANAERP__External_User_Id__c',
    '続柄ID': 'MANAERP__Relationship__c',
    '支払人フラグ': 'MANAERP__Use_Parent_as_Payer__c'
};

// Read and process the CSV file
async function processRelationshipFile() {
    try {
        // Read the input CSV file
        const inputFile = path.join(__dirname, 'HAUFM003.csv');
        const outputFile = path.join(__dirname, '..', 'processed', 'relationship_processed.csv');
        
        const fileContent = fs.readFileSync(inputFile, 'utf-8');
        const records = parse(fileContent, {
            columns: true,
            skip_empty_lines: true
        });

        // Transform the records
        const transformedRecords = records.map(record => {
            const newRecord = {};
            
            // Map the columns according to the new structure
            Object.entries(columnMapping).forEach(([oldKey, newKey]) => {
                if (record[oldKey] !== undefined) {
                    // For 続柄ID, apply the relationship mapping
                    if (oldKey === '続柄ID' && relationshipMapping[record[oldKey]]) {
                        newRecord[newKey] = relationshipMapping[record[oldKey]];
                    } else {
                        newRecord[newKey] = record[oldKey];
                    }
                }
            });
            
            return newRecord;
        });

        // Write the transformed records to a new CSV file
        const output = stringify(transformedRecords, {
            header: true,
            columns: Object.values(columnMapping)
        });

        // Ensure the processed directory exists
        const processedDir = path.join(__dirname, '..', 'processed');
        if (!fs.existsSync(processedDir)) {
            fs.mkdirSync(processedDir);
        }

        fs.writeFileSync(outputFile, output);
        console.log(`Processing complete. Output saved to ${outputFile}`);
        
        // Log statistics
        const totalRecords = transformedRecords.length;
        const transformedCount = transformedRecords.filter(record => 
            relationshipMapping[record['MANAERP__Relationship__c']] !== undefined
        ).length;
        
        console.log(`Total records processed: ${totalRecords}`);
        console.log(`Records transformed: ${transformedCount}`);
        console.log(`Records unchanged: ${totalRecords - transformedCount}`);

    } catch (error) {
        console.error('Error processing file:', error);
    }
}

// Run the script
processRelationshipFile(); 