import { promises as fs } from 'fs';
import path from 'path';
import Papa from 'papaparse';
import _ from 'lodash';

// Constants
const SOURCE_FOLDER = 'split_files';
const TARGET_FOLDER = 'mask_files';
const PLACEHOLDER_DOMAIN = 'withusmanabie.com';

// Define columns that need to be masked
const MASKED_COLUMNS = [
    'Last Name',
    'First Name',
    'Last Name (Phonetic)',
    'First Name (Phonetic)',
    'Phone',
    'Other Phone',
    'portable_tel',
    'tel',
    'Email',
    'portable_email',
    'main_email',
    'Sub Email',
    'Postal Code',
    'City',
    'Street 1',
    'Street 2'
];

// Helper function to generate random characters
function generateRandomChars(length) {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

/**
 * Masks a name according to privacy rules
 * @param {string} name - The name to mask
 * @param {boolean} preserveLength - Whether to preserve the original length
 * @returns {string} - The masked name
 */
function maskName(name, preserveLength = true) {
  if (!name) return '';
  
  // If name is not a string, convert to string
  const nameStr = String(name).trim();
  
  // If empty after trimming, return empty string
  if (!nameStr) return '';
  
  // For very short names (1-2 characters), use fixed mask
  if (nameStr.length <= 2) {
    return generateRandomChars(2);
  }
  
  // For longer names, preserve first character and mask the rest
  // If preserveLength is true, generate exactly the same length as original
  const firstChar = nameStr.charAt(0);
  const maskedPart = preserveLength 
    ? generateRandomChars(nameStr.length - 1)
    : generateRandomChars(8);
    
  return firstChar + maskedPart;
}

/**
 * Masks an email address using a fixed domain
 * @param {string} email - The email to mask
 * @returns {string} - The masked email
 */
function maskEmail(email) {
  if (!email) return '';
  
  // If email is not a string, convert to string
  const emailStr = String(email).trim();
  
  // If empty after trimming, return empty string
  if (!emailStr) return '';
  
  // Check if it's a valid email format
  const emailRegex = /^([^@\s]+)@((?:[-a-z0-9]+\.)+[a-z]{2,})$/i;
  const match = emailStr.match(emailRegex);
  
  if (!match) {
    // If not a standard email format, use basic masking
    if (emailStr.length <= 3) return `${generateRandomChars(3)}@${PLACEHOLDER_DOMAIN}`;
    return emailStr.charAt(0) + generateRandomChars(8) + emailStr.charAt(emailStr.length - 1) + `@${PLACEHOLDER_DOMAIN}`;
  }
  
  // Extract username and domain parts
  const [, username] = match;
  
  // Mask username part, keep first and last character if long enough
  let maskedUsername;
  if (username.length <= 2) {
    maskedUsername = generateRandomChars(username.length);
  } else {
    maskedUsername = username.charAt(0) + 
                     generateRandomChars(username.length - 2) + 
                     username.charAt(username.length - 1);
  }
  
  // Use fixed placeholder domain
  return `${maskedUsername}@${PLACEHOLDER_DOMAIN}`;
}

/**
 * Identifies which column maps to which field type
 * @param {string} columnName - The column name to check
 * @returns {string|null} - The field type or null if not found
 */
function getColumnType(columnName) {
  for (const [type, possibleNames] of Object.entries(COLUMNS_TO_MASK)) {
    if (possibleNames.includes(columnName)) {
      return type;
    }
  }
  return null;
}

// Function to mask a value
function maskValue(value) {
    if (!value || typeof value !== 'string') return value;
    
    // For phone numbers and zip codes
    if (/^\d+$/.test(value)) {
        if (value.length <= 5) return value;
        return value.slice(0, 3) + generateRandomChars(value.length - 5) + value.slice(-2);
    }
    
    // For email addresses
    if (value.includes('@')) {
        return maskEmail(value);
    }
    
    // For addresses and other text
    return maskName(value, true);
}

/**
 * Main function to mask data in a CSV file
 * @param {string} inputFile - Path to the input CSV file
 * @returns {boolean} - True if masking was successful, false otherwise
 */
async function maskData(inputFile) {
    try {
        console.log(`\nStarting data masking for ${inputFile}...`);
        
        // Read the input file
        const content = await fs.readFile(inputFile, 'utf8');
        
        // Parse the CSV
        const results = Papa.parse(content, {
            header: true,
            skipEmptyLines: true
        });
        
        if (results.errors.length > 0) {
            console.error('Error parsing CSV:', results.errors);
            return false;
        }
        
        // Mask sensitive data
        const maskedData = results.data.map(record => {
            const maskedRecord = { ...record };
            MASKED_COLUMNS.forEach(column => {
                if (maskedRecord[column] !== undefined) {
                    maskedRecord[column] = maskValue(maskedRecord[column]);
                }
            });
            return maskedRecord;
        });
        
        // Create output filename
        const outputFile = inputFile.replace('.csv', '_masked.csv');
        
        // Convert back to CSV
        const csv = Papa.unparse(maskedData, {
            header: true
        });
        
        // Write the masked data to a new file
        await fs.writeFile(outputFile, csv, 'utf8');
        
        console.log(`✓ Successfully masked data and saved to ${outputFile}`);
        return {
            success: true,
            totalRecords: maskedData.length,
            maskedColumns: MASKED_COLUMNS,
            outputFile: outputFile
        };
    } catch (error) {
        console.error('Error masking data:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

/**
 * Processes all CSV files in a directory
 * @param {string} sourceFolder - Folder containing source CSV files
 * @param {string} targetFolder - Folder to save masked CSV files
 * @param {boolean} preserveNameLength - Whether to preserve the original length of masked values
 * @returns {Object} - Statistics about the batch processing
 */
async function processAllFiles(sourceFolder, targetFolder, preserveNameLength = true) {
  try {
    // Ensure target directory exists
    await fs.mkdir(targetFolder, { recursive: true });
    
    // Read all files from source directory
    const files = await fs.readdir(sourceFolder);
    const csvFiles = files.filter(file => path.extname(file).toLowerCase() === '.csv');
    
    console.log(`Found ${csvFiles.length} CSV files in ${sourceFolder}`);
    
    if (csvFiles.length === 0) {
      console.log('No CSV files to process.');
      return { processedFiles: 0 };
    }
    
    // Process each CSV file
    const results = [];
    for (const file of csvFiles) {
      const inputPath = path.join(sourceFolder, file);
      const outputPath = path.join(targetFolder, file.replace('.csv', '_masked.csv'));
      
      try {
        const result = await maskData(inputPath);
        results.push(result);
        console.log(`✅ Successfully processed ${file}`);
      } catch (error) {
        console.error(`❌ Failed to process ${file}:`, error.message);
      }
    }
    
    return {
      processedFiles: results.length,
      failedFiles: csvFiles.length - results.length,
      fileResults: results
    };
  } catch (error) {
    console.error('Error processing files:', error);
    throw error;
  }
}

/**
 * Main function that handles both single file and batch processing modes
 */
async function main() {
  // Get command line arguments
  const args = process.argv.slice(2);
  
  // Set default option
  const preserveNameLength = true; // Set to false if you want fixed-length masks
  
  // Check for batch mode flag
  const batchModeIndex = args.indexOf('--batch');
  const batchMode = batchModeIndex !== -1;
  
  if (batchMode) {
    // Remove the --batch flag from args
    args.splice(batchModeIndex, 1);
    
    // Get source and target folders for batch processing
    const sourceFolder = args[0] || SOURCE_FOLDER;
    const targetFolder = args[1] || TARGET_FOLDER;
    
    console.log('🚀 Starting batch data masking process...');
    console.log(`Source folder: ${sourceFolder}`);
    console.log(`Target folder: ${targetFolder}`);
    
    try {
      const result = await processAllFiles(sourceFolder, targetFolder, preserveNameLength);
      
      console.log(`\n📊 Masking summary:`);
      console.log(`- Total files processed: ${result.processedFiles}`);
      console.log(`- Failed files: ${result.failedFiles}`);
      
      if (result.fileResults && result.fileResults.length > 0) {
        console.log(`\n📄 File details:`);
        result.fileResults.forEach((fileResult, index) => {
          console.log(`\n  ${index + 1}. ${path.basename(fileResult.inputFile)}:`);
          console.log(`     - Records processed: ${fileResult.totalRecords}`);
          console.log(`     - Columns masked: ${fileResult.maskedColumns.length}`);
        });
      }
      
      console.log('\n✨ Batch masking complete!');
    } catch (error) {
      console.error('Failed to mask data:', error);
      process.exit(1);
    }
  } else {
    // Single file mode
    const inputFile = args[0];
    if (!inputFile) {
      console.error('Error: Please provide an input file path');
      process.exit(1);
    }
    
    console.log('🚀 Starting single file data masking process...');
    console.log(`Input file: ${inputFile}`);
    
    try {
      const result = await maskData(inputFile);
      
      if (!result.success) {
        console.error('Failed to mask data:', result.error);
        process.exit(1);
      }
      
      console.log(`\n📊 Masking summary:`);
      console.log(`- Total records processed: ${result.totalRecords}`);
      console.log(`- Columns masked: ${result.maskedColumns.join(', ')}`);
      console.log(`- Output saved to: ${result.outputFile}`);
      
      console.log('\n✨ Single file masking complete!');
    } catch (error) {
      console.error('Failed to mask data:', error);
      process.exit(1);
    }
  }
}

// Export the maskData function
export { maskData };

// Only run the main function if this file is being run directly
if (import.meta.url === `file://${process.argv[1]}`) {
    main();
}