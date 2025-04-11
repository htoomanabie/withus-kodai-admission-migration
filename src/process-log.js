import fs from 'fs';
import { createReadStream } from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter } from 'csv-writer';

// Check if input and output file paths are provided
if (process.argv.length < 5) {
    console.error('Please provide input files and output file paths');
    console.error('Usage: node process_csv.js <csv1_with_external_id> <csv2_with_other_columns> <output_file>');
    process.exit(1);
}

const csv1File = process.argv[2]; // File containing External User Id
const csv2File = process.argv[3]; // File containing other columns
const outputFile = process.argv[4];

// Function to read CSV file and return records
const readCSV = (filePath) => {
    return new Promise((resolve, reject) => {
        const records = [];
        createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => records.push(data))
            .on('end', () => resolve(records))
            .on('error', (err) => reject(err));
    });
};

// Main processing function
const processFiles = async () => {
    try {
        // Read both CSV files
        const [csv1Records, csv2Records] = await Promise.all([
            readCSV(csv1File),
            readCSV(csv2File)
        ]);

        // Extract External User Ids from CSV1
        const externalIds = csv1Records.map(record => record['External User Id'] || 'N/A');

        // Combine records
        const combinedRecords = csv2Records.map((record, index) => {
            // Get External User Id from CSV1 at the same index
            const externalId = externalIds[index] || 'N/A';

            // Create new record with all columns from CSV2 and External User Id from CSV1
            return {
                ...record,
                'External User Id': externalId
            };
        });

        // Get headers from the first record
        const headers = Object.keys(combinedRecords[0]).map(key => ({
            id: key,
            title: key
        }));

        // Create CSV writer
        const csvWriter = createObjectCsvWriter({
            path: outputFile,
            header: headers
        });

        // Write records to output file
        await csvWriter.writeRecords(combinedRecords);
        console.log('CSV file has been written successfully');
        console.log(`Processed ${combinedRecords.length} records`);
        console.log(`Used ${externalIds.length} External User Ids from the first file`);

    } catch (err) {
        console.error('Error processing files:', err);
    }
};

// Run the processing
processFiles(); 