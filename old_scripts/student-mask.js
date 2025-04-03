import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';

// File name constants
const DEFAULT_INPUT_FILE = 'student-test.csv';
const DEFAULT_OUTPUT_FILE = 'student-test-masked.csv';

// Column mappings for fields that need to be masked
const COLUMNS_TO_MASK = {
  'lastName': ['Last Name', 'LastName', 'LAST_NAME', 'last_name'],
  'firstName': ['First Name', 'FirstName', 'FIRST_NAME', 'first_name'],
  'lastNamePhonetic': ['Last Name (Phonetic)', 'LastNamePhonetic', 'LAST_NAME_PHONETIC', 'last_name_phonetic'],
  'firstNamePhonetic': ['First Name (Phonetic)', 'FirstNamePhonetic', 'FIRST_NAME_PHONETIC', 'first_name_phonetic'],
  'email': ['Email', 'EMAIL', 'email', 'e_mail', 'E_MAIL', 'main_email'],
  'subEmail': ['Sub Email', 'SubEmail', 'SUB_EMAIL', 'sub_email', 'secondary_email', 'SECONDARY_EMAIL', 'portable_email']
};

// Functions for generating masked values
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

// Updated function to mask email addresses with fixed domain
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
    if (emailStr.length <= 3) return '***@withusmanabie.com';
    return emailStr.charAt(0) + '***' + emailStr.charAt(emailStr.length - 1) + '@withusmanabie.com';
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
  return `${maskedUsername}@withusmanabie.com`;
}

// Helper function to identify which column maps to which field type
function getColumnType(columnName) {
  for (const [type, possibleNames] of Object.entries(COLUMNS_TO_MASK)) {
    if (possibleNames.includes(columnName)) {
      return type;
    }
  }
  return null;
}

// Function to process the CSV file and mask specified columns
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
      console.warn('Warning: Some parsing errors occurred:', results.errors);
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
    
    console.log(`Found ${maskedColumnsCount} columns to mask: ${Object.keys(columnTypes).join(', ')}`);
    
    // Process data and mask the specified columns
    const maskedData = results.data.map((row, index) => {
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
    
    console.log(`Successfully masked ${maskedData.length} records`);
    console.log('Masking complete!');
    
    return {
      totalRecords: maskedData.length,
      maskedColumns: Object.keys(columnTypes),
      outputFile: outputFilePath
    };
  } catch (error) {
    console.error('Error while masking student names:', error);
    throw error;
  }
}

// Example usage
async function main() {
  // Get command line arguments if provided
  const args = process.argv.slice(2);
  const inputFile = args[0] || DEFAULT_INPUT_FILE;
  const outputFile = args[1] || DEFAULT_OUTPUT_FILE;
  const preserveNameLength = true; // Set to false if you want fixed-length masks
  
  try {
    console.log('Starting data masking process...');
    console.log(`Input file: ${inputFile}`);
    console.log(`Output file: ${outputFile}`);
    
    const result = await maskStudentNames(inputFile, outputFile, preserveNameLength);
    
    console.log(`\nMasking summary:`);
    console.log(`- Total records processed: ${result.totalRecords}`);
    console.log(`- Columns masked: ${result.maskedColumns.join(', ')}`);
    console.log(`- Output saved to: ${result.outputFile}`);
  } catch (error) {
    console.error('Failed to mask student data:', error);
    process.exit(1);
  }
}

// Execute the script
main();