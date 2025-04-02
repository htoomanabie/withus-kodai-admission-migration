import { promises as fs } from 'fs';
import path from 'path';
import Papa from 'papaparse';
import _ from 'lodash';

// Constants
const DEFAULT_INPUT_FILE = 'student-test.csv';
const DEFAULT_OUTPUT_FILE = 'student-test-masked.csv';
const SOURCE_FOLDER = 'split_files';
const TARGET_FOLDER = 'mask_files';
const PLACEHOLDER_DOMAIN = 'withusmanabie.com';

// Column mappings for fields that need to be masked
const COLUMNS_TO_MASK = {
  'lastName': ['Last Name', 'LastName', 'LAST_NAME', 'last_name'],
  'firstName': ['First Name', 'FirstName', 'FIRST_NAME', 'first_name'],
  'lastNamePhonetic': ['Last Name (Phonetic)', 'LastNamePhonetic', 'LAST_NAME_PHONETIC', 'last_name_phonetic'],
  'firstNamePhonetic': ['First Name (Phonetic)', 'FirstNamePhonetic', 'FIRST_NAME_PHONETIC', 'first_name_phonetic'],
  'email': ['Email', 'EMAIL', 'email', 'e_mail', 'E_MAIL', 'main_email'],
  'subEmail': ['Sub Email', 'SubEmail', 'SUB_EMAIL', 'sub_email', 'secondary_email', 'SECONDARY_EMAIL', 'portable_email']
};

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
    return '**';
  }
  
  // For longer names, preserve first character and mask the rest
  // If preserveLength is true, generate exactly the same length as original
  const firstChar = nameStr.charAt(0);
  const maskedPart = preserveLength 
    ? '*'.repeat(nameStr.length - 1)
    : '***';
    
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
    if (emailStr.length <= 3) return `***@${PLACEHOLDER_DOMAIN}`;
    return emailStr.charAt(0) + '***' + emailStr.charAt(emailStr.length - 1) + `@${PLACEHOLDER_DOMAIN}`;
  }
  
  // Extract username and domain parts
  const [, username] = match;
  
  // Mask username part, keep first and last character if long enough
  let maskedUsername;
  if (username.length <= 2) {
    maskedUsername = '*'.repeat(username.length);
  } else {
    maskedUsername = username.charAt(0) + 
                     '*'.repeat(username.length - 2) + 
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

/**
 * Processes a single CSV file and masks specified columns
 * @param {string} inputFilePath - Path to the input CSV file
 * @param {string} outputFilePath - Path to save the masked CSV file
 * @param {boolean} preserveLength - Whether to preserve the original length of masked values
 * @returns {Object} - Statistics about the masking operation
 */
async function maskStudentNames(inputFilePath, outputFilePath, preserveLength = true) {
  try {
    console.log(`Reading CSV file: ${inputFilePath}`);
    const csvData = await fs.readFile(inputFilePath, 'utf8');
    
    // Parse CSV
    const results = Papa.parse(csvData, {
      header: true,
      skipEmptyLines: true
    });
    
    if (results.errors.length > 0) {
      console.warn(`Warning: Some parsing errors in ${path.basename(inputFilePath)}:`, results.errors);
    }
    
    // Get headers to identify which columns need masking
    const headers = results.meta.fields;
    console.log(`Detected ${headers.length} columns in the CSV file`);
    
    // Map headers to their column types
    const columnTypes = {};
    let maskedColumnsCount = 0;
    
    headers.forEach(header => {
      const type = getColumnType(header);
      if (type) {
        columnTypes[header] = type;
        maskedColumnsCount++;
      }
    });
    
    console.log(`Found ${maskedColumnsCount} columns to mask in ${path.basename(inputFilePath)}`);
    
    // Process data and mask the specified columns
    const maskedData = results.data.map((row) => {
      const maskedRow = { ...row };
      
      // Apply masking to the identified columns
      Object.entries(columnTypes).forEach(([columnName, type]) => {
        if (type === 'email' || type === 'subEmail') {
          maskedRow[columnName] = maskEmail(row[columnName]);
        } else {
          maskedRow[columnName] = maskName(row[columnName], preserveLength);
        }
      });
      
      return maskedRow;
    });
    
    // Convert back to CSV
    const maskedCsv = Papa.unparse(maskedData, {
      header: true,
      delimiter: ',',
      newline: '\n'
    });
    
    // Write to output file
    console.log(`Writing masked data to: ${outputFilePath}`);
    await fs.writeFile(outputFilePath, maskedCsv, 'utf8');
    
    return {
      totalRecords: maskedData.length,
      maskedColumns: Object.keys(columnTypes),
      inputFile: inputFilePath,
      outputFile: outputFilePath
    };
  } catch (error) {
    console.error(`Error while masking file ${inputFilePath}:`, error);
    throw error;
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
      const outputPath = path.join(targetFolder, file);
      
      try {
        const result = await maskStudentNames(inputPath, outputPath, preserveNameLength);
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
      console.error('Failed to mask student data:', error);
      process.exit(1);
    }
  } else {
    // Single file mode
    const inputFile = args[0] || DEFAULT_INPUT_FILE;
    const outputFile = args[1] || DEFAULT_OUTPUT_FILE;
    
    console.log('🚀 Starting single file data masking process...');
    console.log(`Input file: ${inputFile}`);
    console.log(`Output file: ${outputFile}`);
    
    try {
      const result = await maskStudentNames(inputFile, outputFile, preserveNameLength);
      
      console.log(`\n📊 Masking summary:`);
      console.log(`- Total records processed: ${result.totalRecords}`);
      console.log(`- Columns masked: ${result.maskedColumns.join(', ')}`);
      console.log(`- Output saved to: ${result.outputFile}`);
      
      console.log('\n✨ Single file masking complete!');
    } catch (error) {
      console.error('Failed to mask student data:', error);
      process.exit(1);
    }
  }
}

// Execute the script
main();